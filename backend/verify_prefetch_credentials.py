"""预热回迁调度期限凭据的场景验证。

两条请求指向同一个冷存对象 GLACIER-DEMO-01：
  请求 A: 09:59 受理，受理策略版本窗口 1 分钟 → 调度期限 10:00
          对象恰在 10:00 确认就绪（队列到期与冷存完成撞在同一刻）
  请求 B: 10:03 受理（10 点之后才进来），策略版本窗口 1 分钟 → 期限 10:04

判定规则：就绪确认时刻必须严格早于调度期限；撞在期限同一刻按“队列到期先行”，
A 过期、B 可在期限内读取同一个对象。判定只看冷存侧登记的就绪时刻，不看谁先提交。
随后把默认期限调大，再重开旧任务，确认 A 的历史依据（受理时间、策略版本、期限、
过期原因）仍冻结；对已取得凭据的 B 重复提交，确认读取次数不增加、读取对象不变。

运行：cd backend && .venv/bin/python verify_prefetch_credentials.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services import service_for
from app.services.cold_storage import READ_LEDGER_DIR, READINESS_DIR
from app.services.hot_prefetch import SCHEDULING_DIR
from app.store import store

BASE = datetime(2026, 11, 22, 9, 59, tzinfo=timezone.utc)
T_0959 = BASE
T_1000 = datetime(2026, 11, 22, 10, 0, tzinfo=timezone.utc)
T_1003 = datetime(2026, 11, 22, 10, 3, tzinfo=timezone.utc)
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app", "data", "assets")

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"
passed = 0
failed = 0


def check(desc: str, condition: bool, detail: str = "") -> bool:
    global passed, failed
    if condition:
        print(f"  {GREEN}PASS{RESET} {desc}")
        passed += 1
        return True
    print(f"  {RED}FAIL{RESET} {desc} {detail}")
    failed += 1
    return False


def iso(value: datetime) -> str:
    return value.isoformat()


def build_fixture(asset_a: str, asset_b: str, asset_shared: str) -> tuple[int, int, int]:
    """放入两条冷存任务与两条回迁任务：A/B 指向同一个冷存对象，不同受理时点。"""
    cold_rows = store.rows("cold_storage")
    cold_a = {
        "id": max((int(r.get("id", 0)) for r in cold_rows), default=0) + 1,
        "status": "已完成",
        "pending": False,
        "abnormal": False,
        "任务编号": "COLD-A001",
        "源资产": asset_a,
        "目标存储": "S3-GLACIER",
        "压缩算法": "ZSTD",
        "迁移量": "10GB",
        "预计耗时": "2h",
        "迁移状态": "已完成",
        "校验摘要": "CRC32校验通过",
    }
    cold_rows.append(cold_a)
    cold_b = dict(cold_a, id=cold_a["id"] + 1, 任务编号="COLD-B001", 源资产=asset_b)
    cold_rows.append(cold_b)
    cold_shared = dict(cold_a, id=cold_a["id"] + 2, 任务编号="COLD-S001", 源资产=asset_shared)
    cold_rows.append(cold_shared)

    hot_rows = store.rows("hot_prefetch")
    hot_a = {
        "id": max((int(r.get("id", 0)) for r in hot_rows), default=0) + 1,
        "status": "排队中",
        "pending": True,
        "abnormal": False,
        "任务编号": "HOT_-A001",
        "冷存位置": f"S3://glacier/{asset_shared}",
        "目标分区": f"{asset_a}_hot",
        "回迁量": "10GB",
        "预计耗时": "30m",
        "优先级": "高",
        "过期策略": "T+1分钟",
        "任务状态": "排队中",
        "源资产": asset_shared,
    }
    hot_rows.append(hot_a)
    hot_b = dict(
        hot_a,
        id=hot_a["id"] + 1,
        任务编号="HOT_-B001",
        冷存位置=f"S3://glacier/{asset_shared}",
        目标分区=f"{asset_b}_hot",
        源资产=asset_shared,
        过期策略="T+1分钟",
    )
    hot_rows.append(hot_b)
    return cold_a["id"], hot_a["id"], hot_b["id"]


def snapshot_artifacts() -> set[str]:
    """记录运行前已有的数据产物，脚本结束时只清理本次新增的文件。"""
    paths = set()
    for directory in (SCHEDULING_DIR, READINESS_DIR, READ_LEDGER_DIR, DATA_DIR):
        if os.path.isdir(directory):
            paths.update(os.path.join(directory, f) for f in os.listdir(directory))
    return paths


def cleanup_artifacts(before: set[str]) -> None:
    for directory in (SCHEDULING_DIR, READINESS_DIR, READ_LEDGER_DIR, DATA_DIR):
        if not os.path.isdir(directory):
            continue
        for name in os.listdir(directory):
            path = os.path.join(directory, name)
            if path not in before:
                os.remove(path)


def main() -> int:
    print("=" * 66)
    print("预热回迁调度期限凭据场景验证：09:59 / 10:00 / 10:03")
    print("=" * 66)

    cold = service_for("cold_storage")
    prefetch = service_for("hot_prefetch")
    assert cold is not None and prefetch is not None
    before = snapshot_artifacts()

    asset_a = "ASSE-A-DEMO"
    asset_b = "ASSE-B-DEMO"
    shared = "GLACIER-DEMO-01"
    cold_a_id, hot_a_id, hot_b_id = build_fixture(asset_a, asset_b, shared)

    print("\n=== 场景 0: 受理时冻结策略版本 ===")
    default_version = prefetch._current_policy_version()
    token_a, message_a = prefetch.accept_request(
        hot_a_id, requested_at=T_0959, expiry_policy="T+1分钟", policy_version="policy-T+1@v1")
    print(f"  A 受理: {message_a}")
    check("A 受理成功", token_a is not None)
    check("A 受理时间保留实际时间", token_a["accepted_at"] == iso(T_0959),
          token_a["accepted_at"])
    check("A 策略版本为受理时版本", token_a["policy_version"] == "policy-T+1@v1")
    check("A 调度期限=09:59+1分钟=10:00", token_a["deadline"] == iso(T_1000),
          str(token_a["deadline"]))
    check("默认策略未被改写", prefetch._current_policy_version() == default_version)

    # 10:00：冷存对象恰在调度期限同一刻确认就绪
    cold.record_object_readiness(shared, {"任务编号": "COLD-S001", "目标存储": "S3-GLACIER",
                                          "校验摘要": "CRC32校验通过"}, ready_at=T_1000)
    shared_evidence = cold.get_object_readiness(f"S3://glacier/{shared}")
    check("同一个冷存对象在 10:00 登记就绪", shared_evidence["ready_at"] == iso(T_1000),
          str(shared_evidence))

    print("\n=== 场景 1: 09:59 受理的 A 在临界时刻取凭据 ===")
    result_a, message_a2 = prefetch.execute_acceptance(token_a["token"], now=T_1000)
    print(f"  A 执行: {message_a2}")
    check("A 判为过期（队列到期先行）", result_a["expired"] is True, str(result_a))
    check("A 未取得读取对象", result_a["read_object"] is None)
    check("A 读取次数为 0", result_a["read_count"] == 0)
    check("A 过期原因写明撞在到期时刻", "恰好在到期时刻" in result_a["reason"],
          result_a["reason"])

    print("\n=== 场景 2: 10:03 才进来的 B ===")
    token_b, message_b = prefetch.accept_request(
        hot_b_id, requested_at=T_1003, expiry_policy="T+1分钟", policy_version="policy-T+1@v1")
    print(f"  B 受理: {message_b}")
    check("B 受理时间为 10:03", token_b["accepted_at"] == iso(T_1003))
    check("B 调度期限=10:04", token_b["deadline"] == iso(T_1003 + timedelta(minutes=1)))

    # 同一个对象在 10:00 就已就绪，严格早于 B 的 10:04 期限；
    # 即使 B 的执行实际发生在 10:05，只要就绪时刻在期限内，凭据依然有效。
    result_b, message_b2 = prefetch.execute_acceptance(
        token_b["token"], now=T_1003 + timedelta(minutes=2))
    print(f"  B 执行: {message_b2}")
    check("B 取得读取凭据", result_b["expired"] is False, str(result_b))
    check("B 读取的是同一个冷存对象", result_b["read_object"] == f"S3://glacier/{shared}",
          str(result_b["read_object"]))
    check("B 的凭据指向同一份就绪证据",
          result_b["evidence"]["evidence_id"] == shared_evidence["evidence_id"])
    check("B 读取次数为 1", result_b["read_count"] == 1)

    print("\n=== 场景 3: 同一对象不冒名顶替 ===")
    # 任务 C 声明的目标资产是 ASSE-C-DEMO，但冷存位置指向 A 的对象；
    # 冷存里只有 A 的就绪证据，不能拿来给 C 用。
    hot_rows = store.rows("hot_prefetch")
    asset_c = "ASSE-C-DEMO"
    hot_c = {
        "id": max((int(r.get("id", 0)) for r in hot_rows), default=0) + 1,
        "status": "排队中",
        "pending": True,
        "abnormal": False,
        "任务编号": "HOT_-C001",
        "冷存位置": f"S3://glacier/{shared}",
        "目标分区": f"{asset_c}_hot",
        "回迁量": "10GB",
        "预计耗时": "30m",
        "优先级": "高",
        "过期策略": "T+1分钟",
        "任务状态": "排队中",
        "源资产": asset_c,
    }
    hot_rows.append(hot_c)
    spoof_token, spoof_message = prefetch.accept_request(
        hot_c["id"], requested_at=T_1003, expiry_policy="T+1分钟", policy_version="policy-T+1@v1")
    spoof_result, spoof_exec_message = prefetch.execute_acceptance(
        spoof_token["token"], now=T_1003)
    print(f"  冒名执行: {spoof_exec_message}")
    check("跨对象证据被拒绝", spoof_result["read_object"] is None, str(spoof_result))
    check("拒绝原因指明对象不一致", "不一致" in spoof_result["reason"], spoof_result["reason"])
    check("冒名受理不产生读取次数", spoof_result["read_count"] == 0)

    print("\n=== 场景 4: 调整默认期限后重开旧任务 ===")
    changed = prefetch.set_default_policy("T+2天", version="policy-default@2026-11-22T1010")
    check("默认期限调整成功", changed["ok"] is True, str(changed))
    reopened, reopen_message = prefetch.reopen_acceptance(token_a["token"])
    print(f"  重开 A: {reopen_message}")
    check("A 历史策略版本仍在", reopened["historical_policy_version"] == "policy-T+1@v1")
    check("A 受理时间仍在", reopened["accepted_at"] == iso(T_0959))
    check("A 调度期限仍是 10:00", reopened["deadline"] == iso(T_1000))
    check("A 过期原因仍可回查", "恰好在到期时刻" in reopened["read"]["reason"],
          reopened["read"]["reason"])
    check("A 表明当前默认策略已变化", reopened["policy_version_drift"] is True)
    check("A 读取状态仍为已过期", reopened["read_status"] == "已过期")
    check("策略漂移没有让 A 变成可读", reopened["read"]["read_object"] is None)

    print("\n=== 场景 5: 已取凭据的 B 重复提交 ===")
    first_read_at = result_b["read_at"]
    replay1, replay_message1 = prefetch.execute_acceptance(token_b["token"], now=T_1003)
    replay2, replay_message2 = prefetch.execute_acceptance(token_b["token"], now=T_1003)
    print(f"  重复提交: {replay_message1}")
    check("重复提交返回同一凭据", replay1["token"] == result_b["token"])
    check("重复提交不再读取（次数仍为 1）", replay1["read_count"] == 1)
    check("重复提交读取对象不变", replay1["read_object"] == result_b["read_object"])
    check("重复提交读取时刻冻结", replay1["read_at"] == first_read_at)
    check("重复提交读取记录号不变", replay1["read_id"] == result_b["read_id"])
    check("第三次提交同样不读取", replay2["read_count"] == 1)
    check("两次重放消息都提示未重复读取",
          "未重复读取" in replay_message1 and "未重复读取" in replay_message2)
    check("对象读取台账只有一条",
          sum(1 for a in prefetch.list_acceptances()
              if a["token"] == token_b["token"] and a.get("read")) == 1)
    cold_reads = cold.list_object_reads(f"S3://glacier/{shared}")
    check("冷存侧同一对象实际读取台账只有一条", len(cold_reads) == 1, str(cold_reads))
    check("冷存台账记录读取凭据", cold_reads[0]["credential_token"] == token_b["token"])
    check("冷存台账记录读取对象", cold_reads[0]["object_uri"] == f"S3://glacier/{shared}")
    check("过期受理 A 与冒名受理 C 都没有在冷存侧留下读取",
          [r for r in cold.list_object_reads() if r["credential_token"] != token_b["token"]] == [])

    print("\n=== 场景 6: A 重开后重新提交仍不可读 ===")
    replay_a, replay_a_message = prefetch.execute_acceptance(token_a["token"], now=T_1003)
    print(f"  A 重提: {replay_a_message}")
    check("A 仍不可读", replay_a["read_object"] is None)
    check("A 不增加读取次数", replay_a["read_count"] == 0)

    print("\n=== 场景 7: 任务行与台账可回查 ===")
    row_a = store.find("hot_prefetch", hot_a_id)
    row_b = store.find("hot_prefetch", hot_b_id)
    check("A 行标记过期并带依据", row_a.get("过期") is True and bool(row_a.get("过期依据")))
    check("A 行保留策略版本", row_a.get("策略版本") == "policy-T+1@v1")
    check("B 行带读取凭据与读取对象", row_b.get("读取凭据") == token_b["token"]
          and row_b.get("读取对象") == f"S3://glacier/{shared}")
    ledger = prefetch.list_acceptances()
    check("台账保留受理实际时间与策略版本",
          all("accepted_at" in item and "policy_version" in item for item in ledger))
    check("台账中 A 仍为已过期", any(i["token"] == token_a["token"] and i["state"] == "已过期"
                                      for i in ledger))

    print("\n" + "=" * 66)
    print(f"结果: {GREEN}{passed} 通过{RESET} / {RED}{failed} 失败{RESET} / {passed + failed} 总计")
    print("=" * 66)
    cleanup_artifacts(before)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
