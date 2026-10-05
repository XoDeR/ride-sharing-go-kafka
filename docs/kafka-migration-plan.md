# Plan: Replace RabbitMQ with Kafka

Status: **REVISED after review (all open questions answered, see section 7) – awaiting final approval. No code has been changed.**

## 1. Current state (what we are replacing)

All RabbitMQ code is concentrated in a small surface, which makes this migration tractable.

**Shared layer**
- [shared/messaging/rabbitmq.go](../shared/messaging/rabbitmq.go): connection, one topic exchange `trip`, a DLX `dlx` + `dead_letter_queue`, 7 declared queues, `PublishMessage(ctx, routingKey, msg)`, `ConsumeMessages(queue, handler)` (prefetch 1, manual ack, 3 retries with backoff via [shared/retry](../shared/retry/retry.go), then `Reject(false)` → DLQ).
- [shared/messaging/queue_consumer.go](../shared/messaging/queue_consumer.go): gateway-only consumer that forwards queue messages to WebSockets (auto-ack, `RoutingKey` becomes the WS message `type`).
- [shared/messaging/events.go](../shared/messaging/events.go): queue name constants + payload structs (payload structs stay as-is).
- [shared/contracts/amqp.go](../shared/contracts/amqp.go): `AmqpMessage{OwnerID, Data}` envelope + routing-key constants.
- [shared/tracing/rabbitmq.go](../shared/tracing/rabbitmq.go): OTel inject/extract via AMQP headers.

**Routing today** (routing key → queue → consumer)

| Routing key(s) | Queue | Consumer |
|---|---|---|
| `trip.event.created`, `trip.event.driver_not_interested` | `find_available_drivers` | driver-service ([trip_consumer.go](../services/driver-service/trip_consumer.go)) |
| `driver.cmd.trip_request` | `driver_cmd_trip_request` | api-gateway → driver WS |
| `driver.cmd.trip_accept`, `driver.cmd.trip_decline` | `driver_trip_response` | trip-service ([driver_consumer.go](../services/trip-service/internal/infrastructure/events/driver_consumer.go)) |
| `trip.event.no_drivers_found` | `notify_driver_no_drivers_found` | api-gateway → rider WS |
| `trip.event.driver_assigned` | `notify_driver_assign` | api-gateway → rider WS |
| `payment.cmd.create_session` | `payment_trip_response` | payment-service ([trip_consumer.go](../services/payment-service/internal/events/trip_consumer.go)) |
| `payment.event.session_created` | `notify_payment_session_created` | api-gateway → rider WS |
| `payment.event.success` | `payment_success` (**never declared/bound**, see below) | trip-service ([payment_consumer.go](../services/trip-service/internal/infrastructure/events/payment_consumer.go)) |

**Publishers:** trip-service (`trip_publisher.go`, `driver_consumer.go`), driver-service, payment-service, api-gateway (`ws.go` driver accept/decline, `http.go` Stripe webhook).

**Infra:** `rabbitmq-deployment.yaml` (dev + prod), `rabbitmq-credentials` secret (`RABBITMQ_URI`) referenced by 4 service deployments in each of dev/prod, Tiltfile resource + `resource_deps`, README, `tools/create_service.go` comment text, architecture docs.

### Existing problems found while reading (the migration will touch these, so decide how to handle them)

1. **`payment_success` queue is never declared or bound** in `setupExchangesAndQueues`, so trip-service's payment consumer has no queue to consume from and `payment.event.success` is dropped. In Kafka this just becomes a topic that works; flag it as a behavior change (the "payed" status update will start actually happening).
2. **Gateway consumers are started per WebSocket connection** on shared queues and never stopped. With RabbitMQ competing-consumer semantics, a message for rider A can be delivered to rider B's goroutine and be lost (`ErrConnectionNotFound`), and goroutines leak on disconnect. Kafka needs a different design here (section 4.4); this is the biggest design change in the migration.
3. **DLQ headers are never actually sent.** `ConsumeMessages` mutates `d.Headers` locally and then `Reject(false)`; RabbitMQ doesn't carry those mutations. Our Kafka DLQ will carry the failure metadata properly.
4. `declareAndBindQueue` uses `log.Fatal` on declare error (goes away).

## 2. Decisions (confirmed)

| # | Decision | Recommendation | Why |
|---|---|---|---|
| D1 | Go client | **segmentio/kafka-go** | Pure Go (Dockerfiles/Tilt build with `CGO_ENABLED=0`, which rules out confluent-kafka-go), simple Reader/Writer API, manual commit supported. Alternative: `twmb/franz-go` (more featureful, steeper API). |
| D2 | Broker | **Apache Kafka in KRaft mode** (no ZooKeeper), 1 broker in dev; `apache/kafka` image | Simplest dev footprint. |
| D3 | Topic model | **One topic per routing key** (topic name == existing routing key, e.g. `trip.event.created`) | 1:1 with today's contract; the WS `type` sent to the web client stays identical; no web changes. |
| D4 | Consumer groups replace queues | One group per (service, purpose), see 4.3 | Preserves competing-consumer behavior for services. |
| D5 | Message key | `OwnerID` (rider/driver id) | Per-user ordering within a topic. |
| D6 | Partitions / replication | 3 partitions per topic, replication factor 1, **in both dev and prod** (single broker everywhere) | Allows scaling consumers; matches today's single RabbitMQ node in prod. |
| D7 | Delivery semantics | **At-least-once**, commit offset after handler success | Matches today's manual-ack behavior; requires idempotent handlers (section 6). |
| D8 | DLQ | Single `dead_letter` topic, failure info in headers | Mirrors current single `dead_letter_queue`. |
| D9 | Topic provisioning | Created idempotently at service startup (`EnsureTopics`), broker auto-create disabled | Mirrors today's declare-on-connect; no extra Job needed. |
| D10 | Abstraction | Services stop importing the broker library; they use our own `messaging.Delivery` type | Today every consumer imports `amqp091` directly, which leaks the broker into business code. |
| D11 | Cutover | **Big-bang** on a branch, no dual-run | Project is dev-stage, 4 services, single replicas. |
| D12 | Prod topology and security | **Single broker, PLAINTEXT, no auth**, same as today's prod RabbitMQ setup; the prod manifest carries a prominent comment that SASL/TLS and a multi-broker setup (RF 3, `min.insync.replicas=2`) must be added for real production | Explicit user decision. |
| D13 | Tilt isolation | Namespace **`ride-sharing-go-kafka`**, all host port-forwards remapped (section 4.1) | Must coexist with another Tilt project on the same machine. |
| D14 | Kafka UI | Included (`provectuslabs/kafka-ui`) in dev; replaces the RabbitMQ management console | Explicit user decision. |
| D15 | Services run only in k8s | No external/host listener on Kafka; **no host port-forward for Kafka** | All services and the frontend run in the cluster. |
| D16 | Gateway fan-out | Group-per-instance, start at latest (4.4) | Confirmed. |
| D17 | Stripe | Treat the Stripe flow as real: `payment.event.success` → trip "payed" must work end to end | No working Stripe account is attached today, but the design must assume one. |
| D18 | Old docs | RabbitMQ docs are **rewritten** as Kafka docs, not kept | Explicit user decision. |

## 3. Target design

### 3.1 Topics

| Topic | Producers | Consumer group(s) |
|---|---|---|
| `trip.event.created` | trip-service | `driver-service.find-drivers` |
| `trip.event.driver_not_interested` | trip-service | `driver-service.find-drivers` |
| `trip.event.no_drivers_found` | driver-service | `api-gateway-<instance>` (4.4) |
| `trip.event.driver_assigned` | trip-service | `api-gateway-<instance>` |
| `driver.cmd.trip_request` | driver-service | `api-gateway-<instance>` |
| `driver.cmd.trip_accept` | api-gateway | `trip-service.driver-response` |
| `driver.cmd.trip_decline` | api-gateway | `trip-service.driver-response` |
| `payment.cmd.create_session` | trip-service | `payment-service.create-session` |
| `payment.event.session_created` | payment-service | `api-gateway-<instance>` |
| `payment.event.success` | api-gateway (Stripe webhook) | `trip-service.payment-success` |
| `dead_letter` | any consumer | none (inspect manually / future replay tool) |

Unused constants in [contracts/amqp.go](../shared/contracts/amqp.go) (`PaymentEventFailed`, `PaymentEventCancelled`, `DriverCmdLocation`, `DriverCmdRegister`) are not topics; `DriverCmdRegister`/`Location` are WS-only types. Don't create topics for them.

### 3.2 Message format

Keep the JSON envelope exactly as is (`{"ownerId": "...", "data": <bytes>}`) so payload structs and handlers don't change. Kafka record:
- **Key:** `OwnerID`
- **Value:** JSON envelope
- **Headers:** `content-type: application/json`, OTel `traceparent`/`tracestate`, and on DLQ: `x-death-reason`, `x-original-topic`, `x-original-partition`, `x-original-offset`, `x-retry-count`.
- Routing key is no longer a separate field: it is the **topic name** (`Delivery.Topic`). Consumers that `switch msg.RoutingKey` become `switch msg.Topic`.

### 3.3 New messaging API (`shared/messaging`)

```go
type Delivery struct {            // replaces amqp.Delivery in handler signatures
    Topic   string
    Key     string
    Value   []byte                // JSON envelope
    Headers map[string]string
}
type Handler func(ctx context.Context, d Delivery) error

type Kafka struct { /* brokers, writer */ }
func NewKafka(brokers []string) (*Kafka, error)            // dials, EnsureTopics
func (k *Kafka) PublishMessage(ctx, topic string, msg contracts.Message) error
func (k *Kafka) Consume(groupID string, topics []string, h Handler) error   // non-blocking, starts goroutine
func (k *Kafka) Close()
```

`Consume` loop: `FetchMessage` → extract trace ctx → retry-with-backoff (`retry.WithBackoff`, unchanged) → on success `CommitMessages`; on exhausted retries publish to `dead_letter` (with headers) **then commit**, so a poison message never blocks a partition. Sequential per consumer = same effect as prefetch 1. Context-cancel stops the loop for clean shutdown (RabbitMQ code had none).

Gateway-specific consumer (`QueueConsumer`) is reworked, see 4.4.

## 4. Implementation steps

Each step should leave the repo compiling. Proposed order, one commit per step.

### 4.1 Infra: broker, Kafka UI, Tilt isolation

**Kafka (dev)**
- Add `infra/development/k8s/kafka-deployment.yaml`: StatefulSet (KRaft, single node combined broker+controller, `apache/kafka`), headless + ClusterIP Service on 9092, PVC, readiness probe on 9092, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, advertised listener = in-cluster service DNS name (`kafka:9092`). A single `PLAINTEXT` listener is enough because nothing runs outside the cluster (D15). Resources: ~1Gi limit with `KAFKA_HEAP_OPTS=-Xmx512m`.
- Add `infra/development/k8s/kafka-ui-deployment.yaml`: `provectuslabs/kafka-ui`, `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092`, Service on 8080, `depends` on kafka via Tilt `resource_deps`.
- Config: remove `rabbitmq-credentials` from `secrets.template.yaml`; add `KAFKA_BROKERS: "kafka:9092"` to `app-config.yaml` (ConfigMap, dev and prod) and have the 4 service deployments read it with `configMapKeyRef` (same pattern as `JAEGER_ENDPOINT`). Your local, git-ignored `secrets.yaml` still has the old RabbitMQ secret; it is harmless, but remove it by hand.

**Kafka (prod)**
- Replace `infra/production/k8s/rabbitmq-deployment.yaml` with `kafka-deployment.yaml` (+ Kafka UI only if you want it in prod; default: **not** included). Single broker, PLAINTEXT, no auth, matching the current prod RabbitMQ posture (D12). The file starts with a block comment along the lines of: `# NOT PRODUCTION-GRADE: single broker, replication factor 1, PLAINTEXT with no authentication. Before real production use add SASL/TLS (and network policies), run >= 3 brokers with replication.factor=3 and min.insync.replicas=2, and set explicit retention.`

**Tilt isolation (D13)**
- Namespace `ride-sharing-go-kafka` so object names (`kafka`, `mongodb`, `api-gateway`, secrets, ConfigMaps, PVCs) don't collide with the other Tilt project. Apply it to **every** resource in the Tiltfile (including the existing mongodb, osrm, jaeger, services, web), not just Kafka:
  - `load('ext://namespace', 'namespace_create', 'namespace_inject')`, `namespace_create('ride-sharing-go-kafka')`, and wrap each manifest: `k8s_yaml(namespace_inject(read_file('...'), 'ride-sharing-go-kafka'))`. (If `secrets.yaml` or `app-config.yaml` are loaded via plain `k8s_yaml(path)` today, they must go through the same wrapper, otherwise `secretKeyRef`/`configMapKeyRef` lookups fail across namespaces.)
  - Optional hardening: `allow_k8s_contexts(...)` is **not** touched; and the namespace is for isolation only, short service names like `kafka:9092` keep working inside the namespace.
  - Run Tilt for this project with a distinct web UI port (`tilt up --port 10351`, default 10350 would clash with the other instance). I'll document this in the README.
- **Host port-forwards remapped** so both projects can run at the same time. Tilt syntax `host:container`. Proposed (please adjust if some clash with your other project; I don't know its ports):

  | Resource | Today | Proposed |
  |---|---|---|
  | rabbitmq (5672, 15672) | removed | n/a |
  | kafka | n/a | **none** (D15) |
  | kafka-ui | n/a | `8180:8080` |
  | mongodb | `27017` | `27117:27017` |
  | osrm | `5000` | `5100:5000` |
  | api-gateway | `8081` | `8181:8081` |
  | web | `3000` | `3100:3000` |
  | jaeger | `16686:16686`, `14268:14268` | `16786:16686`, `14368:14268` |

- **Consequences of remapping web (3100) and gateway (8181)** (these are browser-facing, so they are not just a Tiltfile edit):
  - `web/src/constants.ts` fallbacks `http://localhost:8081` / `ws://localhost:8081/ws` → `8181`; `web-deployment.yaml` `NEXT_PUBLIC_API_URL` / `NEXT_PUBLIC_WEBSOCKET_URL` are currently `http://api-gateway:8081`, which a browser can't resolve. I need to check in step 1 how the web image is built (build-time vs runtime env) to see which of the two actually takes effect, and set both to the browser-reachable `localhost:8181` form.
  - `STRIPE_SUCCESS_URL` / `STRIPE_CANCEL_URL` in `app-config.yaml` and the `APP_URL` default in `services/payment-service/cmd/main.go` → `http://localhost:3100`. Prod `app-config.yaml` has the same `localhost:3000` values; I propose leaving prod untouched since it isn't run through Tilt (tell me if you disagree).
  - Stripe webhook forwarding target (`stripe listen --forward-to localhost:8181/webhook/stripe`); update wherever the README/docs mention it.
  - CORS is `*` in `middleware.go`, so no change is needed there.
- Tiltfile: replace the `### RabbitMQ ###` block with `### Kafka ###` (+ Kafka UI resource); replace `'rabbitmq'` with `'kafka'` in the 4 `resource_deps` lists.

### 4.2 Shared: contracts, messaging, tracing
- Add dependency `github.com/segmentio/kafka-go`; remove `rabbitmq/amqp091-go` at the end (`go mod tidy`).
- `shared/contracts/amqp.go` → `messaging.go`: rename `AmqpMessage` → `Message`; rename constants block comment from "Routing keys" to "Topics". **Constant names/values unchanged** to minimize diffs. Verify `web/src/contracts.ts` doesn't depend on the Go type name (it mirrors the WS contract, not the envelope).
- `shared/messaging/events.go`: delete the queue-name constants; add consumer-group constants (`GroupDriverFindDrivers`, `GroupTripDriverResponse`, …) and `DeadLetterTopic`. Payload structs untouched.
- New `shared/messaging/kafka.go` per 3.3 (replaces `rabbitmq.go`): `NewKafka`, `EnsureTopics` (declares the topic list from 3.1 with partitions/replication from env, tolerant of "already exists"), `PublishMessage` (writer with `RequiredAcks: RequireAll`, hash balancer on key, per-message topic, trace injection), `Consume`, `Close`.
- `shared/tracing/rabbitmq.go` → `kafka.go`: `kafkaHeadersCarrier` over `[]kafka.Header`/`map[string]string`; `TracedPublisher` and `TracedConsumer` keep their structure; span names `kafka.publish` / `kafka.consume`, attributes switch to OTel messaging semconv-style (`messaging.system=kafka`, `messaging.destination.name=<topic>`, `messaging.kafka.message.key`). Note: Jaeger searches by operation name will change.
- Unit tests: header carrier round-trip; envelope JSON compatibility; retry→DLQ decision logic behind a small interface so it can be tested without a broker.

### 4.3 Service migrations
Mechanical in each: replace `*messaging.RabbitMQ` with `*messaging.Kafka`, `amqp091.Delivery` with `messaging.Delivery`, `msg.Body` with `msg.Value`, `msg.RoutingKey` with `msg.Topic`, `contracts.AmqpMessage` with `contracts.Message`, `ConsumeMessages(queue, h)` with `Consume(group, topics, h)`, and `env RABBITMQ_URI` with `KAFKA_BROKERS`.

| Service | Files | Consumer group → topics |
|---|---|---|
| trip-service | `cmd/main.go`, `events/trip_publisher.go`, `events/driver_consumer.go`, `events/payment_consumer.go` | `trip-service.driver-response` → accept, decline; `trip-service.payment-success` → `payment.event.success` |
| driver-service | `main.go`, `trip_consumer.go` | `driver-service.find-drivers` → created, driver_not_interested |
| payment-service | `cmd/main.go`, `internal/events/trip_consumer.go` | `payment-service.create-session` → `payment.cmd.create_session` |
| api-gateway | `main.go`, `ws.go`, `http.go` (Stripe webhook publish) | see 4.4 |

Also: `tools/create_service.go` comment text ("Event handling (RabbitMQ)" → Kafka), `services/trip-service/README.md`.

### 4.4 API gateway fan-out redesign (the non-mechanical part)
Problem: WebSocket pushes are per-user, but the gateway may have N instances and users connect to any of them.

Design:
- Start **one** consumer set at gateway startup (not per WebSocket), consuming `trip.event.no_drivers_found`, `trip.event.driver_assigned`, `payment.event.session_created`, `driver.cmd.trip_request`.
- Consumer group id is **unique per gateway instance** (`api-gateway-<hostname/pod name>`), so every instance receives every message (pub/sub semantics), with start offset = **latest** (a restarted gateway should not replay stale notifications to users).
- Handler: unmarshal envelope, look up `OwnerID` in `ConnectionManager`; if no local connection, silently drop (it belongs to another instance or the user is gone). Downgrade `ErrConnectionNotFound` from an error log to debug.
- WS message `type` = topic name (identical to today's routing key), so the web client is unchanged.
- `handleRidersWebSocket` / `handleDriversWebSocket` lose the `queues` loop and the `rb` parameter for consuming (drivers WS still needs the publisher for accept/decline).
- Trade-off: every instance reads all notification traffic. Fine at this scale; if it ever matters, switch to partition-by-user routing with sticky connections. Consequence of unique group ids: every pod restart leaves an orphaned empty group (visible in Kafka UI). Kafka expires committed offsets of empty groups after `offsets.retention.minutes` (default 7 days); set this to something short (e.g. 60) on the dev/prod broker via `KAFKA_OFFSETS_RETENTION_MINUTES`. Offsets for these groups are not used for resuming anyway (start-at-latest), so the gateway consumer should not commit meaningfully; use auto-commit off + `StartOffset: LastOffset`. On a pod restart the new group id is new, so it starts at latest as intended. (Decision D16: group-per-instance, confirmed.)
- `QueueConsumer` is replaced by a `NotificationConsumer` (same file, same `ConnectionManager` dependency).

### 4.5 Cleanup
- Delete `rabbitmq.go`, `shared/tracing/rabbitmq.go`, both `rabbitmq-deployment.yaml` files, `rabbitmq-credentials` references in all 8 deployment manifests (replace with `KAFKA_BROKERS` env var from ConfigMap).
- Remove `amqp091-go` from `go.mod`/`go.sum`.
- README: replace the RabbitMQ deploy/wait instructions with Kafka; ports.
- Docs (D18, rewrite rather than keep history):
  - Replace `docs/architecture/rabbitmq-flow-v0.md` and `rabbitmq-flow-v1.md` with a single `docs/architecture/kafka-flow.md` (mermaid diagram of topics, producers and consumer groups per 3.1, the dead-letter path, and the gateway per-instance fan-out). Delete the two RabbitMQ files via `git rm`.
  - Update `docs/architecture/trip-creation-flow-v1.md` (the only trip-creation doc that mentions RabbitMQ): exchanges/queues/routing keys → topics/consumer groups. `trip-creation-flow-v0.md` doesn't mention RabbitMQ; I'll re-check it when I get there and leave it alone unless it does.
  - Also update `services/trip-service/README.md` and the top-level `README.md` (Kafka deploy/wait steps, the new Tilt port table, `tilt up --port`, Stripe CLI forwarding port).

## 5. Verification

Static: `go build ./...`, `go vet ./...`, `go mod tidy` clean, `grep -ri "amqp\|rabbit"` returns only the historical docs.

Runtime (Tilt in a local cluster, namespace `ride-sharing-go-kafka`), end-to-end happy path, watching topics and consumer groups in Kafka UI (`localhost:8180`). Also confirm the other Tilt project can run concurrently without port or name clashes:
1. Rider requests a trip → `trip.event.created` → driver-service finds a driver → `driver.cmd.trip_request` → driver WS receives it.
2. Driver accepts → `driver.cmd.trip_accept` → trip-service updates trip → `trip.event.driver_assigned` (rider WS gets it) and `payment.cmd.create_session` → payment-service → `payment.event.session_created` (rider WS gets it).
3. Stripe webhook → `payment.event.success` → trip status becomes `payed` (new: previously dropped, see problem 1). The design assumes a real Stripe account (D17). Since none is attached yet, verify with a Stripe test-mode account plus `stripe listen --forward-to localhost:8181/webhook/stripe` once one exists; until then, verify the Kafka leg by posting a correctly signed `checkout.session.completed` payload (signed with a local `STRIPE_WEBHOOK_KEY` using Stripe's signing scheme) to the gateway, or by producing `payment.event.success` directly in Kafka UI. Stripe retries webhooks and may deliver duplicates, so also confirm that a duplicate `payment.event.success` leaves the trip in `payed` without error.
4. Decline path → `trip.event.driver_not_interested` → another driver search; zero drivers → `trip.event.no_drivers_found` to rider.
5. Failure path: force a handler error (e.g. malformed payload published manually) → 3 retries → record in `dead_letter` with headers; confirm the partition continues to make progress.
6. Restart a service mid-flow: no lost messages, no duplicate side effects beyond what section 6 describes.
7. Two riders connected at once: each receives only their own notifications (regression test for problem 2 above).
8. Traces: one trace spans publish → consume across services in Jaeger.

I found no existing automated tests covering the messaging layer, so the unit tests in 4.2 are new, and the rest is manual E2E.

## 6. Risks and things to watch

- **Idempotency (at-least-once).** A crash between handler success and offset commit causes redelivery. Most sensitive: payment-service `CreatePaymentSession` (could create two Stripe sessions → mitigation: pass the trip ID as a Stripe idempotency key, or check for an existing session per trip) and trip-service `UpdateTrip` (idempotent by nature: setting a status). Driver-assignment publishes could duplicate to the rider; harmless but visible.
- **Per-partition head-of-line blocking** while retrying (up to ~7s with default backoff). Acceptable at this scale; same as prefetch=1 today.
- **Consumer group rebalances** pause consumption briefly during deploys.
- **Topic ordering across topics** isn't guaranteed (e.g. `driver_assigned` vs `create_session`); current flows don't depend on it.
- **Kafka memory footprint** (JVM) is larger than RabbitMQ's 512Mi request; dev gets ~1Gi limit with `KAFKA_HEAP_OPTS=-Xmx512m`. Kafka UI adds roughly another few hundred Mi.
- **Prod durability/security:** single broker with RF 1 means a broker/disk loss loses messages, and PLAINTEXT with no auth is accepted for now (D12); the prod manifest comment makes this explicit.
- **Port remap fallout:** the web/gateway remap touches frontend URLs, Stripe redirect URLs and docs (4.1); missing one of them shows up as a blank/unreachable UI or broken Stripe redirects, so these are listed in the verification run.
- **Startup race:** services call `EnsureTopics` concurrently; creation must tolerate `TopicAlreadyExists`. Services should retry the initial broker dial (use `retry.WithBackoff`), since today they fail hard if the broker isn't up yet.
- **Rollback:** everything is on a branch; the RabbitMQ implementation stays in git history. No data migration is needed since messages are transient.

## 7. Answers to the earlier open questions (resolved)

| Q | Your answer | Reflected in |
|---|---|---|
| Q1 client | kafka-go | D1 |
| Q2 gateway fan-out | group-per-instance, start at latest | D16, 4.4 |
| Q3 prod topology/auth | single broker in dev and prod; no auth now, with a comment in the prod manifest that it must be added for real prod | D6, D12, 4.1 |
| Q4 host-run services | none, everything incl. frontend runs in k8s | D15, 4.1 (no Kafka port-forward) |
| Q5 Kafka UI | yes | D14, 4.1 |
| Q6 old RabbitMQ docs | rewrite as Kafka docs, don't keep | D18, 4.5 |
| Q7 `payed` update starting to work | OK; assume a real Stripe account | D17, section 5 step 3 |
| Q8 Tilt namespace and ports | namespace `ride-sharing-go-kafka`; remap host ports | D13, 4.1 |

### Small points where I chose defaults (tell me only if you disagree)
- The concrete remapped host ports in the table in 4.1 (I can't see your other project's ports).
- Prod `app-config.yaml` keeps its `localhost:3000` Stripe URLs (not remapped), since prod isn't run through Tilt.
- No Kafka UI in the prod manifests.
- Tilt for this project is started with `tilt up --port 10351`.

## 8. Suggested commit sequence

1. `infra: add Kafka (dev + prod manifests), Tilt, config` 
2. `shared: add Kafka messaging + tracing, contracts rename` (+ unit tests)
3. `trip-service: migrate to Kafka`
4. `driver-service: migrate to Kafka`
5. `payment-service: migrate to Kafka`
6. `api-gateway: migrate to Kafka, per-instance notification consumers`
7. `cleanup: remove RabbitMQ code, manifests, dependency; update docs`
