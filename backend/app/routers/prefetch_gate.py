"""预热回迁门禁接口：受理、期限凭据、读取执行、重开。

单独成一个 router，在通用资源路由之前注册，避免 /api/{module}/{entry_id}
把 acceptances / readiness 这类固定路径当成 entry_id 解析。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import (
    AcceptancePayload,
    ActionResult,
    ExecutePayload,
    PolicyPayload,
    ReadinessPayload,
)
from app.services import service_for

router = APIRouter(prefix="/api", tags=["预热回迁调度"])


def parse_moment(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=400, detail=f"时间格式不合法：{value}")


def prefetch_service():
    service = service_for("hot_prefetch")
    if service is None:
        raise HTTPException(status_code=404, detail="未知业务模块：hot_prefetch")
    return service


def cold_service():
    service = service_for("cold_storage")
    if service is None:
        raise HTTPException(status_code=404, detail="未知业务模块：cold_storage")
    return service


@router.get("/hot_prefetch/acceptances", response_model=dict)
def list_acceptances() -> dict[str, Any]:
    items = prefetch_service().list_acceptances()
    return {"items": items, "total": len(items)}


@router.post("/hot_prefetch/acceptances", response_model=ActionResult)
def accept_request(payload: AcceptancePayload) -> ActionResult:
    acceptance, message = prefetch_service().accept_request(
        payload.task_id,
        requested_at=parse_moment(payload.requested_at),
        policy_version=payload.policy_version,
        expiry_policy=payload.expiry_policy,
    )
    return ActionResult(ok=acceptance is not None, message=message, entry=acceptance)


@router.get("/hot_prefetch/acceptances/{token}", response_model=dict)
def get_acceptance(token: str) -> dict[str, Any]:
    acceptance, message = prefetch_service().reopen_acceptance(token)
    if acceptance is None:
        raise HTTPException(status_code=404, detail=message)
    return acceptance


@router.post("/hot_prefetch/acceptances/{token}/execute", response_model=ActionResult)
def execute_acceptance(token: str, payload: ExecutePayload) -> ActionResult:
    result, message = prefetch_service().execute_acceptance(
        token, now=parse_moment(payload.now))
    return ActionResult(ok=result is not None, message=message, entry=result)


@router.post("/hot_prefetch/policy", response_model=ActionResult)
def set_default_policy(payload: PolicyPayload) -> ActionResult:
    result = prefetch_service().set_default_policy(payload.expiry_policy, payload.version)
    return ActionResult(
        ok=bool(result.get("ok")), message=str(result["message"]), entry=result)


@router.get("/cold_storage/readiness", response_model=dict)
def list_readiness() -> dict[str, Any]:
    items = cold_service().list_readiness_evidence()
    return {"items": items, "total": len(items)}


@router.get("/cold_storage/object_reads", response_model=dict)
def list_object_reads(cold_location: str | None = Query(default=None)) -> dict[str, Any]:
    items = cold_service().list_object_reads(cold_location)
    return {"items": items, "total": len(items)}


@router.post("/cold_storage/readiness", response_model=ActionResult)
def confirm_readiness(payload: ReadinessPayload) -> ActionResult:
    service = cold_service()
    entry = service.get_entry(payload.task_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"冷存任务 {payload.task_id} 不存在")
    evidence = service.record_object_readiness(
        str(entry.get("源资产", "")), entry, ready_at=parse_moment(payload.ready_at))
    return ActionResult(
        ok=True,
        message=f"冷存对象 {evidence['object_uri']} 已于 {evidence['ready_at']} 登记就绪",
        entry=evidence,
    )
