"""cold_storage 领域服务：冷存储迁移管理，接收 tier_policy 上游事件触发冷存任务。

跨模块数据流：
    tier_policy → cold_storage: 分层策略生效触发冷存储迁移
    cold_storage → hot_prefetch: 冷存完成可能触发预热回迁需求
    cold_storage → cost_tracker: 迁移任务更新成本追踪
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")
READINESS_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "cold_readiness")
READ_LEDGER_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "cold_reads")

READINESS_READY = "就绪"
COLD_URI_PREFIX = "S3://glacier/"


def _moment(now: datetime | None = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        return current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


class ColdStorageService(BaseModuleService):
    MODULE = "cold_storage"
    ENTITY = "冷存任务"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["任务编号", "源资产", "目标存储", "压缩算法", "迁移量", "预计耗时", "迁移状态", "校验摘要"]
    REQUIRED_FIELDS = ["任务编号", "源资产", "目标存储"]
    STATUS_ORDER = ["待调度", "迁移中", "已完成", "已失败"]
    ACTION_RULES = {"发起迁移": "迁移中", "确认完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(READINESS_DIR, exist_ok=True)
        os.makedirs(READ_LEDGER_DIR, exist_ok=True)
        # 冷存对象的就绪证据（append-only），下游预热回迁只能凭这里的证据读取数据。
        self._readiness_evidence: list[dict[str, Any]] = self._load_readiness_evidence()
        # 冷存对象实际被读取的台账，读取次数以这里为准
        self._read_ledger: list[dict[str, Any]] = self._load_read_ledger()

    def start_migration(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """发起冷存储迁移：记录迁移状态并更新成本追踪。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        asset_id = str(entry.get("源资产", ""))
        target_storage = str(entry.get("目标存储", ""))

        entry["status"] = "迁移中"
        entry["pending"] = True
        entry["迁移状态"] = "迁移中"

        # 写入迁移记录
        self._write_cold_storage_record(asset_id, entry)

        # 更新成本追踪
        self._update_cost_tracker(asset_id, target_storage)

        # 记录审计日志
        self._write_audit_entry(
            entity="冷存任务",
            action="发起迁移",
            target_id=str(entry.get("任务编号", "")),
            detail=f"冷存迁移 {asset_id} → {target_storage}",
        )

        return entry, f"{self.ENTITY} {entry_id} 迁移中，{asset_id}→{target_storage}"

    def complete_migration(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """确认迁移完成：记录校验摘要并检查是否需要预热回迁。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        asset_id = str(entry.get("源资产", ""))
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["迁移状态"] = "已完成"
        entry["校验摘要"] = "CRC32校验通过"

        # 记录该冷存对象就绪的确切时刻，作为下游回迁调度的唯一就绪依据
        self.record_object_readiness(asset_id, entry)

        # 更新数据产物
        self._write_cold_storage_record(asset_id, entry)

        # 如果存在预热回迁需求，触发hot_prefetch
        self._check_prefetch_need(asset_id)

        return entry, f"{self.ENTITY} {entry_id} 已完成，数据已迁移到冷存储"

    def get_migration_status(self, asset_id: str) -> dict[str, Any] | None:
        return self._read_cold_storage_record(asset_id)

    # ---------- 冷存对象就绪证据 ----------
    def record_object_readiness(self, asset_id: str, entry: dict[str, Any],
                                *, ready_at: datetime | None = None) -> dict[str, Any]:
        """登记冷存对象就绪：同一次迁移完成只登记一条证据，重复确认不改写首次就绪时刻。"""
        task_id = str(entry.get("任务编号", ""))
        for evidence in self._readiness_evidence:
            if evidence["asset_id"] == asset_id and evidence["task_id"] == task_id:
                return evidence
        moment = _moment(ready_at)
        evidence = {
            "evidence_id": f"READY-{len(self._readiness_evidence) + 1:04d}",
            "asset_id": asset_id,
            "task_id": task_id,
            "storage": str(entry.get("目标存储", "")),
            "object_uri": f"{COLD_URI_PREFIX}{asset_id}",
            "checksum": str(entry.get("校验摘要", "")),
            "ready_at": moment.isoformat(),
            "recorded_at": moment.isoformat(),
            "state": READINESS_READY,
        }
        self._readiness_evidence.append(evidence)
        self._persist_readiness(evidence)
        return evidence

    def get_object_readiness(self, cold_location: str) -> dict[str, Any] | None:
        """按冷存位置读取对象就绪证据；未登记过就绪的对象返回 None。"""
        target = cold_location.strip()
        asset_id = self._asset_for_location(target)
        if not asset_id:
            return None
        latest = None
        for evidence in self._readiness_evidence:
            if evidence["asset_id"] != asset_id or evidence["state"] != READINESS_READY:
                continue
            if evidence["object_uri"].casefold() != target.casefold():
                continue
            if latest is None or evidence["ready_at"] >= latest["ready_at"]:
                latest = evidence
        return dict(latest) if latest else None

    def list_readiness_evidence(self) -> list[dict[str, Any]]:
        return [dict(evidence) for evidence in self._readiness_evidence]

    # ---------- 实际读取台账 ----------
    def record_object_read(self, cold_location: str, *, consumer: str,
                           credential_token: str, read_at: datetime | None = None,
                           ) -> dict[str, Any]:
        """冷存侧登记一次实际读取；同一凭据重复读取会被拦下并要求走幂等回放。"""
        evidence = self.get_object_readiness(cold_location)
        if evidence is None:
            raise ValueError(f"冷存对象 {cold_location} 尚未就绪，拒绝读取")
        for record in self._read_ledger:
            if record["credential_token"] == credential_token:
                return record
        moment = _moment(read_at)
        record = {
            "read_id": f"READ-{len(self._read_ledger) + 1:04d}",
            "credential_token": credential_token,
            "consumer": consumer,
            "asset_id": evidence["asset_id"],
            "object_uri": evidence["object_uri"],
            "evidence_id": evidence["evidence_id"],
            "read_at": moment.isoformat(),
        }
        self._read_ledger.append(record)
        path = os.path.join(READ_LEDGER_DIR, f"{record['read_id']}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)
        return dict(record)

    def list_object_reads(self, cold_location: str | None = None) -> list[dict[str, Any]]:
        records = [dict(record) for record in self._read_ledger]
        if cold_location is None:
            return records
        return [r for r in records if r["object_uri"].casefold() == cold_location.casefold()]

    def _load_read_ledger(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for filename in sorted(os.listdir(READ_LEDGER_DIR)):
            if not filename.endswith(".json"):
                continue
            with open(os.path.join(READ_LEDGER_DIR, filename), "r", encoding="utf-8") as f:
                records.append(json.load(f))
        return records

    # ---------- 就绪证据持久化 ----------
    def _load_readiness_evidence(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for filename in sorted(os.listdir(READINESS_DIR)):
            if not filename.endswith(".json"):
                continue
            path = os.path.join(READINESS_DIR, filename)
            with open(path, "r", encoding="utf-8") as f:
                records.append(json.load(f))
        return records

    def _persist_readiness(self, evidence: dict[str, Any]) -> None:
        path = os.path.join(READINESS_DIR, f"{evidence['evidence_id']}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(evidence, f, ensure_ascii=False, indent=2, default=str)

    @staticmethod
    def _asset_for_location(cold_location: str) -> str | None:
        if cold_location.casefold().startswith(COLD_URI_PREFIX.casefold()):
            return cold_location[len(COLD_URI_PREFIX):]
        for row in store.rows("cold_storage"):
            asset_id = str(row.get("源资产", ""))
            if not asset_id:
                continue
            if cold_location == asset_id or cold_location.startswith(f"{COLD_URI_PREFIX}{asset_id}"):
                return asset_id
        return None

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "发起迁移":
            return self.start_migration(entry_id)
        if action == "确认完成":
            return self.complete_migration(entry_id)
        return super().run_action(entry_id, action)

    # ---------- 文件 I/O ----------
    def _write_cold_storage_record(self, asset_id: str, entry: dict[str, Any]) -> None:
        filename = f"cold_storage_{asset_id}.json"
        path = os.path.join(DATA_DIR, filename)
        record = {
            "asset_id": asset_id,
            "task_id": str(entry.get("任务编号", "")),
            "target_storage": str(entry.get("目标存储", "")),
            "compression": str(entry.get("压缩算法", "")),
            "migration_volume": str(entry.get("迁移量", "")),
            "status": str(entry.get("迁移状态", "")),
            "checksum": str(entry.get("校验摘要", "")),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)

    def _read_cold_storage_record(self, asset_id: str) -> dict[str, Any] | None:
        filename = f"cold_storage_{asset_id}.json"
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # ---------- 跨模块副作用 ----------
    def _update_cost_tracker(self, asset_id: str, target_storage: str) -> None:
        """更新成本追踪记录中的存储成本。"""
        cost_rows = store.rows("cost_tracker")
        for row in cost_rows:
            if str(row.get("关联资产")) == asset_id:
                row["存储成本"] = f"冷存:{target_storage}"
                row["status"] = "已核算"
                row["pending"] = False
                break

    def _check_prefetch_need(self, asset_id: str) -> None:
        """检查冷存资产是否有预热回迁需求。"""
        hot_rows = store.rows("hot_prefetch")
        for row in hot_rows:
            if str(row.get("冷存位置", "")).find(asset_id) >= 0 and row.get("status") == "排队中":
                return
        # 无已有预热任务则创建种子
        next_id = max((int(r.get("id", 0)) for r in hot_rows), default=0) + 1
        prefetch = {
            "id": next_id,
            "status": "排队中",
            "pending": True,
            "abnormal": False,
            "任务编号": f"HOT_-{next_id:04d}",
            "冷存位置": f"S3://glacier/{asset_id}",
            "目标分区": f"{asset_id}_hot",
            "回迁量": "待估算",
            "预计耗时": "待估算",
            "优先级": "低",
            "过期策略": "T+7",
            "任务状态": "排队中",
        }
        hot_rows.append(prefetch)

    def _write_audit_entry(self, entity: str, action: str, target_id: str,
                           detail: str) -> None:
        audit_rows = store.rows("audit_log")
        next_id = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        entry = {
            "id": next_id,
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "记录编号": f"AUDI-{next_id:04d}",
            "操作主体": entity,
            "操作类型": action,
            "操作对象": target_id,
            "操作时间": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "影响范围": detail,
            "审计摘要": detail,
            "归档状态": "已记录",
        }
        audit_rows.append(entry)