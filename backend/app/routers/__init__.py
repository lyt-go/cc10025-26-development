"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

预热回迁门禁路由必须先注册，固定路径才不会被 /api/{module}/{entry_id} 抢先匹配。
"""
from __future__ import annotations

from app.routers import prefetch_gate, resource

ROUTERS = [prefetch_gate, resource]
