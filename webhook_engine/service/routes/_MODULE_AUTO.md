# routes/
<!-- AUTO-GENERATED. Do not edit. Run gen_module_auto.py to update. -->

## deliveries.py
```
# Delivery operations — inspection, manual redelivery, test ping.


cls DeliveryView(BaseModel)

cls DeliveryIdRequest(BaseModel)

cls SubscriptionIdRequest(BaseModel)

_view(record: DeliveryRecord) -> DeliveryView

_admin(deps: ServiceDeps) -> DeliveryAdminService

_auth(key: str, deps: ServiceDeps) -> None

async list_deliveries(subscription_id: str, limit: int = 50, offset: int = 0, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> Page[DeliveryView]

async get_delivery(id: str, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> DeliveryView

async redeliver(body: DeliveryIdRequest, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> DeliveryView

async ping(body: SubscriptionIdRequest, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> DeliveryView

```

## emit.py
```
# POST /v1/emit — an HTTP producer path into the same event store.


cls EmitRequest(BaseModel)

cls EmitResponse(BaseModel)

async emit_event(body: EmitRequest, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> EmitResponse

```

## health.py
```
# Liveness and readiness probes.


async liveness() -> dict[str, str]

async readiness(response: Response, deps: ServiceDeps = Depends(get_deps)) -> dict[str, str]

```

## subscriptions.py
```
# Subscription management — /v1/subscriptions/*.


cls CreateSubscriptionRequest(BaseModel)

cls SubscriptionIdRequest(BaseModel)

cls SubscriptionResponse(BaseModel)

_to_resp(sub: Subscription, include_secret: bool = False) -> SubscriptionResponse

_auth(key: str, deps: ServiceDeps) -> None

async _require(deps: ServiceDeps, sub_id: str) -> Subscription

async create_subscription(body: CreateSubscriptionRequest, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> SubscriptionResponse

async list_subscriptions(owner_id: str, limit: int = 50, offset: int = 0, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> Page[SubscriptionResponse]

async get_subscription(id: str, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> SubscriptionResponse

async delete_subscription(body: SubscriptionIdRequest, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> SubscriptionResponse

async pause_subscription(body: SubscriptionIdRequest, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> SubscriptionResponse

async resume_subscription(body: SubscriptionIdRequest, x_source_key: str = ..., deps: ServiceDeps = Depends(get_deps)) -> SubscriptionResponse

```
