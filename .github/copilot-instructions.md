# mini-aws Architecture Rules

When generating code for the `mini-aws` project, always adhere to the following rules:

1. **Package Manager**:
   - MUST use `pnpm` for all commands, scripts, and package installations. Do not use `npm` or `yarn` (or `npx`).

2. **Microservices (Command Side)**:
   - Must use a Kafka Consumer to listen to `<domain>-commands-topic`.
   - Must process the command (which ends in `_START`).
   - Must persist the changes to Postgres using `kysely` in the `db.ts` file.
   - Must publish a resulting Domain Event that ends in `_END` to the `<domain>-topic` using a Kafka Producer.
   - Do NOT use HTTP to communicate between services.

3. **API Gateway**:
   - Built with Express.
   - Endpoints (POST/PUT) must implement Idempotency checking using `lmdb` and the `Idempotency-Key` header.
   - Must take REST payload, map to a Command from `shared-contracts` whose `commandType` ends with `_START` (e.g. `CREATE_CUSTOMER_START`), and publish to `<domain>-commands-topic`.
   - Must return HTTP 202 Accepted.
   - Read endpoints (GET) MUST query **Redis** using `ioredis` (e.g. `redis.hget`). Do NOT read `lmdb` from the gateway.

4. **CQRS Engine (View Side)**:
   - Must consume multiple `<domain>-topic`s to build state.
   - Must only process events that end in `_END`.
   - Must save reference state data locally using `lmdb` for stream joining.
   - When a driver event occurs, it must synchronously join data from `lmdb` to calculate a materialized view.
   - The final materialized view MUST be written over the network to **Redis** using `ioredis` (e.g. `redis.hset('domain_view')`).

5. **Shared Contracts**:
   - All Event and Command TypeScript interfaces MUST be located in the `shared-contracts` package.

6. **REST Client API Files**:
   - Every time a new service or API endpoint is handled, a `.http` file (compatible with the VS Code REST Client extension, ID: `humao.rest-client`) must be created or updated for that service.
   - Place `.http` files inside the `api-gateway` folder (e.g., `api-gateway/customers.http`) to easily test the endpoints. Include sample payloads and `Idempotency-Key` headers.

7. **Style**:
   - Use TypeScript.
   - Use ES Modules (`import`/`export`).
   - Prefix terminal logs with an emoji representing the service.

8. **Testing**:
   - Every model and service MUST have tests. 
   - Use the native `node:test` module combined with `tsx`.
   - You MUST write End-to-End (E2E) tests that verify the entire CQRS lifecycle (API Gateway POST -> Wait -> API Gateway GET).
