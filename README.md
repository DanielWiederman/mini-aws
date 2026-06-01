# mini-aws — Event-Driven Architecture Mockup

> A hands-on mockup project that replicates AWS-style distributed systems using **Apache Kafka**, **Node.js**, **TypeScript**, and **BullMQ** — entirely on your local machine.

---

## What Is This?

**mini-aws** is a learning/prototype project that simulates the core concepts behind production-grade AWS microservice architectures without requiring any cloud account or real infrastructure.

It mimics the following AWS primitives locally:

| Local Component | AWS Equivalent |
|---|---|
| Confluent Kafka (Docker) | Amazon MSK (Managed Streaming for Kafka) |
| `customers-service` | Cognito / DynamoDB Streams (user profile & RBAC events) |
| `catalog-service` | DynamoDB Streams / EventBridge (product inventory events) |
| `BullMQ` (backed by Redis) | Amazon SQS / EventBridge Scheduler (delayed price updates) |
| `orders-service` | SQS / EventBridge (order checkout commands) |
| `derived-view-service` | Lambda + Redis (CQRS materialized view processor) |
| `assets-service` | Amazon S3 / MediaConvert (image uploads & dynamic WebP processing) |
| `shared-contracts` | AWS Schema Registry (shared event type contracts) |
| `store` (Next.js) | Public Storefront Application |
| `store-management` (Vite) | Internal Admin Dashboard |
| `otel-collector` | AWS X-Ray (Distributed Tracing & Metrics) |

---

## Current Architecture

The system is built around the **Distributed Saga**, **CQRS (Command Query Responsibility Segregation)**, and **Event Sourcing** patterns. Three independent worker services process commands and stream domain events onto dedicated Kafka topics. A CQRS engine consumes these streams into a queryable read model (Redis) which is exposed via an **API Gateway**.

### Key New Features
* **Background Scheduling:** Price updates can be scheduled into the future using `BullMQ` and `Redis`. The `catalog-service` acts as a highly resilient background worker.
* **Unified RBAC:** The `customers-service` handles all identity (both `CUSTOMER`, `ADMIN`, and `SUPER_ADMIN`), sharing the same pipeline for the Next.js Storefront and the Vite Store Management Dashboard.
* **Idempotency:** The API Gateway ensures safe command retries by enforcing strict `Idempotency-Key` tracking backed by LMDB.
* **Asset Pipeline:** An `assets-service` uses `multer` and `sharp` to process uploaded files directly into optimized WebP images and thumbnails.

### The Observability Stack
The entire system is fully instrumented with **OpenTelemetry**. Traces, Spans, and Service Performance Metrics (SPM) flow through an OTel Collector into **Prometheus** and are visualized beautifully in **Jaeger v2**. Every HTTP request, Postgres query, and Kafka message (via custom manual context propagation) is tracked.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                          mini-aws  —  Current Flow                         │
│                                                                            │
│   PRODUCERS (Write / Command Side)          KAFKA BROKER (Docker)          │
│   ─────────────────────────────             ──────────────────────         │
│                                                                            │
│  ┌─────────────────────┐                  ┌──────────────────────┐         │
│  │  customers-service  │ ─── publish ───► │   customer-topic     │         │
│  │  (Node.js / TS)     │                  └──────────┬───────────┘         │
│  │  CustomerEvent      │                             │                     │
│  └─────────────────────┘                             │                     │
│                                                      │                     │
│  ┌─────────────────────┐                  ┌──────────▼───────────┐         │
│  │  catalog-service    │ ─── publish ───► │   catalog-topic      │         │
│  │  (Node.js / TS)     │                  └──────────┬───────────┘         │
│  │  CatalogEvent       │                             │                     │
│  └─────────────────────┘                             │                     │
│                                                      ▼                     │
│  ┌─────────────────────┐                  ┌──────────────────────┐         │
│  │  orders-service     │ ─── publish ───► │   orders-topic       │         │
│  │  (Node.js / TS)     │                  └──────────┬───────────┘         │
│  │  OrderEvent         │                             │                     │
│  └─────────────────────┘                             │                     │
│                                                      │                     │
│   ─────────────────────────────────────────          │                     │
│   CONSUMER (Read / Query Side)                       │                     │
│   ─────────────────────────────────────────          │                     │
│                                                      ▼                     │
│                                        ┌─────────────────────────┐         │
│                                        │   derived-view-service  │         │
│                                        │   CQRS Engine           │         │
│                                        │   ─────────────────     │         │
│                                        │   • customerTable  ◄────┤ sub     │
│                                        │   • catalogTable   ◄────┤ sub     │
│                                        │   • join + enrich  ◄────┘ sub     │
│                                        │   • apply tier rules    |         │
│                                        │   ─────────────────     │         │
│                                        │   ► Materialized View   │         │
│                                        │     (in-memory store)   │         │
│                                        └─────────────────────────┘         │
│                                                                            │
│   shared-contracts  ─── TypeScript interfaces shared across all services   │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
mini-aws/                        ← monorepo root
├── package.json                 # root scripts (dev, build, typecheck)
├── pnpm-workspace.yaml          # declares all workspace packages
├── tsconfig.base.json           # shared TS compiler settings
├── docker-compose.yaml          # Kafka broker + topic init
│
├── backend/                     # Backend microservices
│   ├── api-gateway/             # Central REST API gateway (Auth & Idempotency)
│   ├── assets-service/          # Image processing and WebP conversion
│   ├── catalog-service/         # Streams product & price events (BullMQ)
│   ├── customers-service/       # Streams user profile & RBAC events
│   ├── derived-view-service/    # CQRS consumer: joins streams → read model
│   ├── logs-service/            # Centralized logging service
│   ├── orders-service/          # Streams checkout / order commands
│   └── shared-contracts/        # Shared event types & DTOs
│
├── store/                       # Next.js 16 storefront application (Port 3001)
│
└── store-management/            # React + Vite internal dashboard (Port 5178)
```

---

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) + Docker Compose
- [Node.js](https://nodejs.org/) 18+ (20+ Recommended)
- [pnpm](https://pnpm.io/)

### 1. Install all dependencies

Run once from the **repo root**:

```bash
pnpm install
```

### 2. Start Infrastructure (Kafka & DBs)

```bash
docker-compose up -d
```

### 3. Run all services & frontends

```bash
pnpm dev
```

This starts all backend microservices, the API Gateway (port 3000), the Next.js Storefront (port 3001), the Assets Service (port 3002), and the Vite Admin Dashboard (port 5178).

### 4. View Frontends & Dashboards

- **Store Management Dashboard:** `http://localhost:5178` (Login with seeded admin user)
- **Public Storefront:** `http://localhost:3001`
- **Jaeger UI (Tracing):** `http://localhost:16686`

---

## Key Concepts Demonstrated

| Concept | Where |
|---|---|
| **Event Sourcing** | Each service publishes immutable domain events to a topic |
| **CQRS** | Producers own the write side; `derived-view-service` owns the read side |
| **Stream-Table Join** | `cqrs-engine.ts` maintains local KTables and joins on order arrival |
| **Delayed Execution** | `catalog-service` uses BullMQ to schedule precise future events |
| **RBAC / Unified Auth** | `customers-service` secures the API Gateway with JWT & LMDB caching |
| **Idempotency** | Prevent duplicate POSTs using `Idempotency-Key` headers |
| **No inter-service HTTP** | Services communicate exclusively through Kafka — zero coupling |

---

## Roadmap

### Phase 4 — Frontend Integration (Completed)
- Next.js storefront consuming the materialized Redis views via API Gateway.
- Internal Vite dashboard with beautiful dark-mode glassmorphism.
- Soft-deletes, schedule price changes, and product updates.
- `assets-service` for handling image uploads dynamically (Multer + Sharp).

### Phase 5 — Cloud-Ready (Up Next)
- Replace Docker Kafka with **AWS MSK**
- Deploy services as **AWS ECS / Lambda** functions
- Use **AWS API Gateway** in front of the REST layer
- Add **CloudWatch** observability and alerting

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | TypeScript (Node.js) |
| API Gateway | Express |
| Messaging | Apache Kafka (KafkaJS client) |
| Background Jobs | BullMQ |
| Database | PostgreSQL (Kysely), Redis (ioredis), & LMDB (Idempotency Cache) |
| File Processing | Multer & Sharp (WebP compression) |
| Tracing & Metrics | OpenTelemetry, Prometheus, Jaeger v2 |
| Infrastructure | Docker Compose |
| Monorepo | pnpm workspaces |
| Frontends | React, Next.js, Vite |

---

## License

MIT
