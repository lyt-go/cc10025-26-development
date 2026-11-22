"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class AcceptancePayload(BaseModel):
    """一次回迁受理：任务号、实际受理时间、当时的策略版本与期限。"""

    task_id: int
    requested_at: str | None = None
    policy_version: str | None = None
    expiry_policy: str | None = None


class ExecutePayload(BaseModel):
    """凭受理 token 执行读取；now 用于回放指定时刻。"""

    now: str | None = None


class PolicyPayload(BaseModel):
    """调整默认期限，只影响之后的受理。"""

    expiry_policy: str
    version: str | None = None


class ReadinessPayload(BaseModel):
    """冷存对象就绪确认：task_id 为冷存任务 id，ready_at 为确认就绪时刻。"""

    task_id: int
    ready_at: str | None = None
