"""标本事件快处服务向 API 和 CLI 暴露的稳定错误。"""


class CollectionDispatchError(RuntimeError):
    code = "traffic_error"
    status = 400


class NotFound(CollectionDispatchError):
    code = "not_found"
    status = 404


class Conflict(CollectionDispatchError):
    code = "conflict"
    status = 409


class InventoryInsufficient(Conflict):
    """所属站点该种类物资的物理库存无法满足申请数量。"""

    code = "inventory_insufficient"


class InventoryIncompatible(Conflict):
    """站点虽有同种类库存，但批次不满足兼容等级、有效期或可用状态约束。"""

    code = "inventory_incompatible"


class Forbidden(CollectionDispatchError):
    code = "forbidden"
    status = 403


class InvalidState(CollectionDispatchError):
    code = "invalid_state"
    status = 409


class ValidationFailed(CollectionDispatchError):
    code = "validation_failed"
    status = 422
