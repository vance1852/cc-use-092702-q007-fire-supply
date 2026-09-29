"""标本事件快处服务向 API 和 CLI 暴露的稳定错误。"""

from __future__ import annotations

from typing import Any, Mapping


class CollectionDispatchError(RuntimeError):
    code = "traffic_error"
    status = 400

    def __init__(self, detail: str | Mapping[str, Any] = "") -> None:
        if isinstance(detail, Mapping):
            self.details: dict[str, Any] = dict(detail)
            message = str(self.details.get("message", ""))
        else:
            self.details = {}
            message = str(detail)
        super().__init__(message)


class NotFound(CollectionDispatchError):
    code = "not_found"
    status = 404


class Conflict(CollectionDispatchError):
    code = "conflict"
    status = 409


class Forbidden(CollectionDispatchError):
    code = "forbidden"
    status = 403


class InvalidState(CollectionDispatchError):
    code = "invalid_state"
    status = 409


class InventoryShortage(CollectionDispatchError):
    """候选批次满足任务约束，但可用数量不足以完成本次出库。"""

    code = "inventory_shortage"
    status = 409


class InventoryIncompatible(CollectionDispatchError):
    """站内有库存，但没有任何批次同时满足种类、兼容等级、有效期或可用状态约束。"""

    code = "inventory_incompatible"
    status = 422


class ValidationFailed(CollectionDispatchError):
    code = "validation_failed"
    status = 422
