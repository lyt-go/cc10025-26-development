"""hot_prefetch 领域服务：预热回迁管理，接收 tier_policy 和 cold_storage 上游事件。

跨模块数据流：
    tier_policy → hot_prefetch: COLD→HOT策略变更触发预热回迁
    cold_storage → hot_prefetch: 读取冷存对象就绪证据，核定回迁调度期限
    hot_prefetch → cost_tracker: 回迁任务更新成本追踪

调度语义：
    一条「受理」= 一次对目标分区的回迁请求，受理时按当时的策略版本快照算出一个
    绝对读期限 deadline，并绑定一个凭据 token。deadline 只是“调度期限”，数据能不能
    真正读，还要看 cold_storage 里同一个冷存对象在读取时刻是否已经登记就绪。
    - 就绪证据里的确认时刻严格早于 deadline → 可执行，即使执行提交得更晚也有效；
    - 就绪确认时刻等于或晚于 deadline → 该受理过期，之后确认或调整默认期限都不改判；
    - 对同一受理重复提交：幂等，不重复读取，返回同一凭据与同一份读取结果。
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.cold_storage import ColdStorageService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")
SCHEDULING_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "prefetch_scheduling")

DEFAULT_POLICY_VERSION = "policy-default@2026-11"
DEFAULT_WINDOW_MINUTES = 60
_MINUTE_TOKENS = ("T+", "MIN", "分钟", "MINUTE")
_HOUR_TOKENS = ("H", "小时", "HOUR")
_DAY_TOKENS = ("D+", "天", "DAY")


def _moment(now: datetime | None = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        return current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def policy_window_minutes(expiry_policy: str) -> tuple[int, str] | None:
    """把过期策略解析成从受理时刻起算的调度窗口（分钟）。

    兼容 T+7 / T+7D / 7天 / 60分钟 / T+2H 这类写法；解析不出窗口返回 None，
    表示该策略版本只登记了原文、没有可核定绝对期限的窗口。
    """
    text = str(expiry_policy or "").strip().upper()
    if not text:
        return None
    match = re.search(r"(\d+)", text)
    if match is None:
        return None
    amount = int(match.group(1))
    if any(token in text for token in _MINUTE_TOKENS):
        return amount, "minutes"
    if any(token in text for token in _HOUR_TOKENS):
        return amount * 60, "hours"
    if any(token in text for token in _DAY_TOKENS):
        return amount * 24 * 60, "days"
    # 仅写数字（如 "7"）时按天处理，与 T+7 的既有约定一致
    return amount * 24 * 60, "days"


class HotPrefetchService(BaseModuleService):
    MODULE = "hot_prefetch"
    ENTITY = "回迁任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "冷存位置", "目标分区", "回迁量", "预计耗时", "优先级",
                   "过期策略", "任务状态", "受理时间", "策略版本", "调度期限", "读取对象"]
    REQUIRED_FIELDS = ["任务编号", "冷存位置", "目标分区"]
    STATUS_ORDER = ["排队中", "回迁中", "已完成", "已取消"]
    ACTION_RULES = {"发起回迁": "回迁中", "确认完成": "已完成", "取消任务": "已取消"}
    NEGATIVE_ACTIONS = []

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(SCHEDULING_DIR, exist_ok=True)
        # 受理台账（append-only）：每项受理都保留实际受理时间与当时的策略快照
        self._acceptances: list[dict[str, Any]] = []
        # 读取结果：同一受理只落一次，重复提交不再触发读取
        self._reads: dict[str, dict[str, Any]] = {}
        self._cold_storage: ColdStorageService | None = None
        self._pending_default_policy: dict[str, Any] | None = None

    # ---------- 上游依赖 ----------
    @property
    def cold_storage(self) -> ColdStorageService:
        if self._cold_storage is None:
            from app.services import service_for
            service = service_for("cold_storage")
            if not isinstance(service, ColdStorageService):
                raise RuntimeError("冷存储服务未就绪，无法核对对象就绪证据")
            self._cold_storage = service
        return self._cold_storage

    # ---------- 受理 ----------
    def accept_request(
        self,
        entry_id: int,
        *,
        requested_at: datetime | None = None,
        policy_version: str | None = None,
        expiry_policy: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """受理一次回迁请求：冻结当时的策略快照并核定回迁调度期限凭据。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        accepted_at = _moment(requested_at)
        entity_policy = str(entry.get("过期策略", "")).strip()
        if expiry_policy is not None:
            effective_policy = str(expiry_policy).strip()
        elif entity_policy and policy_window_minutes(entity_policy) is not None:
            effective_policy = entity_policy
        else:
            effective_policy = f"T+{DEFAULT_WINDOW_MINUTES}分钟"
        version = policy_version or self._current_policy_version()
        cold_location = str(entry.get("冷存位置", ""))
        target_partition = str(entry.get("目标分区", ""))

        window = policy_window_minutes(effective_policy)
        deadline = None
        if window is not None:
            deadline = accepted_at + timedelta(minutes=window[0])

        seq = len(self._acceptances) + 1
        acceptance = {
            "acceptance_id": f"ACPT-{seq:04d}",
            "token": f"PREF-{seq:04d}",
            "task_id": entry_id,
            "任务编号": str(entry.get("任务编号", "")),
            "cold_location": cold_location,
            "target_partition": target_partition,
            "accepted_at": accepted_at.isoformat(),
            "policy_version": version,
            "expiry_policy": effective_policy,
            "policy_window_minutes": window[0] if window else None,
            "policy_window_unit": window[1] if window else None,
            "deadline": deadline.isoformat() if deadline else None,
            "state": "已受理",
            "credential": None,
            "read": None,
        }
        self._acceptances.append(acceptance)
        self._sync_entry(entry, acceptance)
        self._persist_acceptance(acceptance)

        expiry_text = deadline.isoformat() if deadline else "策略未给出可核定期限"
        return acceptance, (
            f"{self.ENTITY} {entry_id} 已受理，策略版本 {version}，"
            f"调度期限 {expiry_text}"
        )

    def list_acceptances(self) -> list[dict[str, Any]]:
        """受理台账：过期项也能回查当时适用的策略版本与窗口。"""
        return [json.loads(json.dumps(item, ensure_ascii=False, default=str))
                for item in self._acceptances]

    def get_acceptance(self, token: str) -> dict[str, Any] | None:
        for acceptance in self._acceptances:
            if acceptance["token"] == token:
                return json.loads(json.dumps(acceptance, ensure_ascii=False, default=str))
        return None

    # ---------- 执行 ----------
    def execute_acceptance(
        self,
        token: str,
        *,
        now: datetime | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """凭受理 token 执行回迁：先核就绪证据，再决定是否真正读取。

        重复提交同一 token 直接返回第一次的结果，不再读取冷存对象。
        """
        acceptance = self._find_mutable(token)
        if acceptance is None:
            return None, f"受理凭据 {token} 不存在"

        previous_read = self._reads.get(acceptance["token"])
        if previous_read is not None:
            return previous_read, "该受理已执行过，返回首次读取凭据（未重复读取）"

        if acceptance["credential"] is None:
            self._credentialize(acceptance)

        outcome = self._decide(acceptance, _moment(now))
        result = {
            "acceptance_id": acceptance["acceptance_id"],
            "token": acceptance["token"],
            "task_id": acceptance["task_id"],
            "任务编号": acceptance["任务编号"],
            "target_partition": acceptance["target_partition"],
            "policy_version": acceptance["policy_version"],
            "accepted_at": acceptance["accepted_at"],
            "deadline": acceptance["deadline"],
            "evidence": outcome.get("evidence", acceptance["credential"].get("evidence")),
            "evidence_asset": acceptance["credential"].get("evidence_asset"),
            "evidence_ready_at": acceptance["credential"].get("evidence_ready_at"),
            "read_asset": outcome["read_asset"],
            "read_object": outcome["read_object"],
            "read_id": outcome.get("read_id"),
            "read_at": outcome["read_at"],
            "read_count": outcome["read_count"],
            "expired": outcome["expired"],
            "state": outcome["state"],
            "reason": outcome["reason"],
        }

        if outcome["state"] == "待就绪":
            # 对象还没登记就绪：不是最终决定，不占用读取次数，等就绪后可再次提交
            acceptance["state"] = "待就绪"
            acceptance["credential"] = None
            self._persist_acceptance(acceptance)
            entry = store.find(self.MODULE, acceptance["task_id"])
            if entry is not None:
                self._sync_entry(entry, acceptance)
            return result, f"受理 {acceptance['acceptance_id']} 尚不可读取：{outcome['reason']}"

        acceptance["state"] = outcome["state"]
        acceptance["read"] = result
        # 最终决定（已读取 / 已过期）只落一次，重复提交直接回放，不再读冷存
        self._reads[acceptance["token"]] = result
        self._persist_acceptance(acceptance)

        if outcome["read_object"]:
            entry = store.find(self.MODULE, acceptance["task_id"])
            if entry is not None:
                entry["读取凭据"] = acceptance["token"]
                entry["读取对象"] = outcome["read_object"]
                self._sync_entry(entry, acceptance)
            return result, (
                f"受理 {acceptance['acceptance_id']} 取得读取凭据，"
                f"读取 {outcome['read_asset']}"
            )
        entry = store.find(self.MODULE, acceptance["task_id"])
        if entry is not None:
            self._sync_entry(entry, acceptance)
        return result, (
            f"受理 {acceptance['acceptance_id']} 不可执行：{outcome['reason']}"
        )

    # ---------- 凭据内部逻辑 ----------
    def _credentialize(self, acceptance: dict[str, Any]) -> None:
        """回查同一冷存对象的就绪证据；证据属于另一个对象时绝不冒名顶替。"""
        entry = store.find(self.MODULE, acceptance["task_id"])
        cold_location = acceptance["cold_location"]
        expected_asset = None
        if entry is not None:
            expected_asset = str(entry.get("源资产") or "").strip() or None
        if expected_asset is None:
            expected_asset = self._asset_from_location(cold_location)

        evidence = self.cold_storage.get_object_readiness(cold_location)
        same_object = self._same_object(cold_location, expected_asset, evidence)
        credential = {
            "token": acceptance["token"],
            "task_id": acceptance["task_id"],
            "policy_version": acceptance["policy_version"],
            "deadline": acceptance["deadline"],
            "expected_asset": expected_asset,
            "expected_object": cold_location,
            "evidence": evidence,
            "evidence_asset": evidence["asset_id"] if evidence else None,
            "evidence_ready_at": evidence["ready_at"] if evidence else None,
            "evidence_object": evidence["object_uri"] if evidence else None,
            "same_object": same_object,
            "usable": False,
            "reason": "",
        }
        if evidence is None:
            credential["reason"] = f"冷存对象 {cold_location} 尚无就绪证据"
        elif expected_asset and evidence["asset_id"] != expected_asset:
            credential["reason"] = (
                f"就绪证据属于 {evidence['asset_id']}，与本次受理目标 {expected_asset} 不一致"
            )
        elif not same_object:
            credential["reason"] = (
                f"就绪证据对象 {evidence['object_uri']} 与本次受理目标 {cold_location} 不一致"
            )
        else:
            credential["usable"] = True
            credential["reason"] = "就绪证据与受理凭据指向同一冷存对象"
        acceptance["credential"] = credential

    @staticmethod
    def _same_object(cold_location: str, expected_asset: str | None,
                     evidence: dict[str, Any] | None) -> bool:
        if evidence is None:
            return False
        if expected_asset and evidence.get("asset_id") != expected_asset:
            return False
        return str(evidence.get("object_uri", "")).casefold() == cold_location.casefold()

    def _decide(self, acceptance: dict[str, Any], now: datetime) -> dict[str, Any]:
        """用“调度期限 + 冷存就绪确认时刻”判定是否还能执行。

        判定顺序就是业务冲突的裁决顺序：
        1. 调度期限 = 受理时刻 + 受理时策略版本给出的窗口，受理后不再改写；
        2. 冷存对象必须已登记就绪，且就绪证据指向本次受理的同一个对象；
        3. 就绪确认时刻必须严格早于调度期限，才认为“在期限前就已就绪”；
           恰好同一刻属于队列到期与冷存完成撞车，按“到期先行”裁决（期限为上界、不含等号）；
        4. 就绪确认时刻晚于等于调度期限，受理已过期，之后无论谁再确认、再怎么调
           默认期限，都不能把旧决定改成可执行。
        """
        credential = acceptance["credential"]
        deadline = self._parse_moment(acceptance["deadline"])

        if credential["evidence"] is None:
            # 对象尚未登记就绪：受理还没到能下结论的时候（无论期限是否已过，
            # 都等就绪证据落库后再判），下结论时只认证据里的就绪时刻。
            return self._pending(now, credential["reason"])
        if not credential["usable"]:
            return self._expired(now, credential["reason"])

        ready_at = self._parse_moment(credential["evidence_ready_at"])
        if deadline is None:
            # 策略没有给出可核定期限：不敢开工，按不可执行处理，保证过期项可回查
            return self._expired(now, "受理时的策略版本未给出可核定期限")

        if ready_at is None:
            return self._pending(now, "就绪证据缺少确认时刻，无法核定")

        if ready_at >= deadline:
            relation = "恰好在到期时刻" if ready_at == deadline else "晚于调度期限"
            return self._expired(
                now,
                f"对象就绪确认时间 {credential['evidence_ready_at']} {relation} "
                f"{acceptance['deadline']}，队列到期先行，该受理已过期",
            )

        asset_id = credential["expected_asset"] or credential["evidence_asset"]
        # 真正读取时到冷存储侧落一次读取台账；同一凭据只会产生一条记录
        read_record = self.cold_storage.record_object_read(
            acceptance["cold_location"],
            consumer="hot_prefetch",
            credential_token=acceptance["token"],
            read_at=now,
        )
        return {
            "state": "已读取",
            "expired": False,
            "read_asset": asset_id,
            "read_object": read_record["object_uri"],
            "read_id": read_record["read_id"],
            "read_at": read_record["read_at"],
            "read_count": 1,
            "reason": (
                f"对象在调度期限前已就绪（{credential['evidence_ready_at']} < "
                f"{acceptance['deadline']}），凭据有效"
            ),
            "evidence": credential["evidence"],
        }

    def _pending(self, now: datetime, reason: str) -> dict[str, Any]:
        return {
            "state": "待就绪",
            "expired": None,
            "read_asset": None,
            "read_object": None,
            "read_id": None,
            "read_at": now.isoformat(),
            "read_count": 0,
            "reason": reason,
        }

    def _expired(self, now: datetime, reason: str) -> dict[str, Any]:
        return {
            "state": "已过期",
            "expired": True,
            "read_asset": None,
            "read_object": None,
            "read_id": None,
            "read_at": now.isoformat(),
            "read_count": 0,
            "reason": reason,
        }

    # ---------- 策略版本与重开 ----------
    def set_default_policy(
        self,
        expiry_policy: str = f"T+{DEFAULT_WINDOW_MINUTES}分钟",
        version: str | None = None,
    ) -> dict[str, Any]:
        """调整默认期限：只影响之后的受理，已发出的凭据保持原样。"""
        if policy_window_minutes(expiry_policy) is None:
            return {"ok": False, "message": f"默认期限 {expiry_policy} 无法核定调度窗口"}
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        new_version = version or f"policy-default@{stamp}"
        previous = self._current_policy_version()
        state = self._load_default_policy()
        state.update({
            "expiry_policy": expiry_policy,
            "policy_version": new_version,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        self._pending_default_policy = state
        with open(self._policy_state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, default=str)
        return {
            "ok": True,
            "message": f"默认期限已由 {previous} 调整为 {new_version}（{expiry_policy}）",
            "policy_version": new_version,
            "expiry_policy": expiry_policy,
        }

    def reopen_acceptance(self, token: str) -> tuple[dict[str, Any] | None, str]:
        """重新打开旧受理：历史依据（策略版本、受理时间、期限、凭据）原样冻结。"""
        acceptance = self.get_acceptance(token)
        if acceptance is None:
            return None, f"受理凭据 {token} 不存在"
        current = self._current_policy_version()
        acceptance["current_default_policy"] = current
        acceptance["historical_policy_version"] = acceptance["policy_version"]
        acceptance["policy_version_drift"] = current != acceptance["policy_version"]
        acceptance["read_status"] = {
            "已读取": "已读取",
            "已过期": "已过期",
            "待就绪": "待就绪",
        }.get(str(acceptance.get("state")), "尚未执行")
        return acceptance, (
            f"受理 {acceptance['acceptance_id']} 仍按 {acceptance['policy_version']} 受理，"
            f"当前默认策略 {current}"
        )

    # ---------- 工具 ----------
    def _find_mutable(self, token: str) -> dict[str, Any] | None:
        for acceptance in self._acceptances:
            if acceptance["token"] == token:
                return acceptance
        return None

    def _sync_entry(self, entry: dict[str, Any], acceptance: dict[str, Any]) -> None:
        """把受理/凭据状态回写到任务行，列表页可直接看到策略版本与期限。"""
        entry["受理编号"] = acceptance["acceptance_id"]
        entry["受理时间"] = acceptance["accepted_at"]
        entry["策略版本"] = acceptance["policy_version"]
        entry["调度期限"] = acceptance["deadline"] or ""
        entry["过期"] = acceptance["state"] == "已过期"
        entry["过期依据"] = (
            acceptance["read"]["reason"]
            if entry["过期"] and acceptance.get("read") else ""
        )
        if acceptance["state"] == "已读取":
            entry["status"] = "回迁中"
            entry["pending"] = True
            entry["任务状态"] = "回迁中"
        elif acceptance["state"] == "已过期":
            entry["status"] = "已取消"
            entry["pending"] = False
            entry["任务状态"] = "已取消"
        else:
            entry["status"] = "排队中"
            entry["pending"] = True
            entry["任务状态"] = "排队中"

    @staticmethod
    def _parse_moment(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return _moment(datetime.fromisoformat(str(value)))
        except ValueError:
            return None

    @staticmethod
    def _asset_from_location(cold_location: str) -> str | None:
        if cold_location.casefold().startswith("s3://glacier/"):
            return cold_location[len("S3://glacier/"):]
        return None

    @property
    def _policy_state_path(self) -> str:
        return os.path.join(SCHEDULING_DIR, "default_policy.json")

    def _load_default_policy(self) -> dict[str, Any]:
        pending = getattr(self, "_pending_default_policy", None)
        if pending is not None:
            return pending
        path = self._policy_state_path
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        state = {
            "expiry_policy": f"T+{DEFAULT_WINDOW_MINUTES}分钟",
            "policy_version": DEFAULT_POLICY_VERSION,
            "updated_at": None,
        }
        self._pending_default_policy = state
        return state

    def _current_policy_version(self) -> str:
        return str(self._load_default_policy().get("policy_version") or DEFAULT_POLICY_VERSION)

    def _persist_acceptance(self, acceptance: dict[str, Any]) -> None:
        state = self._load_default_policy()
        with open(self._policy_state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, default=str)
        path = os.path.join(SCHEDULING_DIR, f"{acceptance['acceptance_id']}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(acceptance, f, ensure_ascii=False, indent=2, default=str)
        self._write_prefetch_record(acceptance["cold_location"], {
            "任务编号": acceptance["任务编号"],
            "目标分区": acceptance["target_partition"],
            "优先级": "",
            "过期策略": acceptance["expiry_policy"],
            "任务状态": acceptance["state"],
        })

    # ---------- 既有动作 ----------
    def start_prefetch(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """发起回迁（管理动作）：未持有效凭据时只排队，不读取数据。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        token = entry.get("受理编号") and self._token_for_entry(entry)
        if token:
            result, message = self.execute_acceptance(token)
            if result is not None and result.get("read_object"):
                return entry, message
            return entry, f"{self.ENTITY} {entry_id} 暂不具备读取资格：{message}"

        entry["status"] = "排队中"
        entry["pending"] = True
        entry["任务状态"] = "排队中"
        return entry, f"{self.ENTITY} {entry_id} 尚未受理，不具备读取资格，请先登记受理"

    def complete_prefetch(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        if not entry.get("读取凭据"):
            return entry, f"{self.ENTITY} {entry_id} 未取得读取凭据，不能确认完成"

        cold_location = str(entry.get("冷存位置", ""))
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["任务状态"] = "已完成"

        self._write_prefetch_record(cold_location, entry)
        return entry, f"{self.ENTITY} {entry_id} 已完成，数据已从冷存回迁到热层"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "发起回迁":
            return self.start_prefetch(entry_id)
        if action == "确认完成":
            return self.complete_prefetch(entry_id)
        return super().run_action(entry_id, action)

    def _token_for_entry(self, entry: dict[str, Any]) -> str | None:
        acceptance_id = str(entry.get("受理编号", ""))
        for acceptance in self._acceptances:
            if acceptance["acceptance_id"] == acceptance_id:
                return acceptance["token"]
        return None

    def _write_prefetch_record(self, cold_location: str, entry: dict[str, Any]) -> None:
        safe_key = cold_location.replace("/", "_").replace(":", "_")
        filename = f"hot_prefetch_{safe_key}.json"
        path = os.path.join(DATA_DIR, filename)
        record = {
            "cold_location": cold_location,
            "task_id": str(entry.get("任务编号", "")),
            "target_partition": str(entry.get("目标分区", "")),
            "volume": str(entry.get("回迁量", "")),
            "priority": str(entry.get("优先级", "")),
            "expiry_policy": str(entry.get("过期策略", "")),
            "status": str(entry.get("任务状态", "")),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)

    def _update_cost_for_prefetch(self, cold_location: str) -> None:
        cost_rows = store.rows("cost_tracker")
        for row in cost_rows:
            if cold_location.find(str(row.get("关联资产", ""))) >= 0:
                row["传输成本"] = "预热回迁中"
                row["status"] = "已核算"
                row["pending"] = False
                break
