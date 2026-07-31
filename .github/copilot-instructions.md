# Copilot instructions

## Repository scope and purpose

This repository implements the Microsoft SQL Server and Azure SQL storage provider for the Durable Task Framework and Durable Functions. It contains:

- `src/DurableTask.SqlServer`: the core orchestration service, SQL client mappings, and embedded database scripts.
- `src/DurableTask.SqlServer.AzureFunctions`: the in-process Azure Functions provider integration.
- `src/Functions.Worker.Extensions.DurableTask.SqlServer`: the isolated-worker extension.
- `test`: integration, Azure Functions, and performance tests.
- `docs`: user-facing architecture, deployment, scaling, and multitenancy documentation.

The provider persists orchestration and entity state, coordinates distributed workers, and supports multiple independently deployed applications sharing one database. Correctness, rolling-upgrade compatibility, and predictable SQL performance take priority over convenience.

## Build and test conventions

- Build with `dotnet build`.
- Run the core suite with `dotnet test --no-build --filter Category!=Stress test/DurableTask.SqlServer.Tests/DurableTask.SqlServer.Tests.csproj`.
- Run the Functions suite with `dotnet test --no-build test/DurableTask.SqlServer.AzureFunctions.Tests/DurableTask.SqlServer.AzureFunctions.Tests.csproj`.
- Integration tests require SQL Server. Locally they use `localhost`; CI starts the container with `test/setup.ps1`.
- Prefer the smallest targeted test first, then run the affected suite.
- Add regression tests for customer-reported failures and schema compatibility changes.

## Code conventions

- Follow the existing C# style, nullable annotations, naming, and file organization.
- Preserve `netstandard2.0` compatibility in the core provider.
- Keep changes focused and avoid unrelated refactoring.
- Use existing logging and retry helpers rather than adding broad catches or silent fallbacks.
- Keep synchronous data-reader loops where comments identify them as deliberate performance optimizations.
- Avoid adding database round trips, metadata queries, dynamic SQL, or per-operation schema checks to hot paths.

## SQL schema conventions

- SQL scripts under `src/DurableTask.SqlServer/Scripts` are embedded in the provider assembly.
- Never edit a published `schema-x.y.z.sql` migration. Add a new idempotent migration for persistent schema changes.
- `logic.sql` and `permissions.sql` are reapplied during schema initialization and upgrades.
- Use explicit column lists for inserts and projections. New physical table columns must be nullable or have safe defaults so older clients continue to work.
- Preserve established transaction table-access order, locking hints, and indexes unless a measured and tested change requires otherwise.

### Published TVPs are immutable compatibility contracts

User-defined table types used as table-valued parameters (TVPs), including `InstanceIDs`, `MessageIDs`, `HistoryEvents`, `OrchestrationEvents`, and `TaskEvents`, are wire contracts between deployed client binaries and the database. SQL Server validates the TVP shape before entering a stored procedure. Adding, removing, reordering, renaming, or changing a column can therefore break older apps even when the column is nullable.

Customers commonly deploy multiple Function Apps independently against the same database resource and schema. The first app to start with a newer provider upgrades that shared schema; apps still running an older provider must continue to operate.

- Do not change or recreate a published TVP in a minor or patch release.
- To carry new data, prefer scalar procedure parameters when practical. Otherwise introduce a versioned TVP and versioned procedure entry point while retaining the old contract.
- A TVP contract may be removed or changed only in an explicitly planned major/breaking release with migration guidance.
- Do not add runtime schema guards to compensate for TVP drift; they run too late to prevent SQL Server's parameter-binding failure and can regress performance.
- Keep `PublishedTableValuedParameterContractsRemainCompatible` frozen to the published baseline. Do not update its expected shapes to make a schema change pass.

## Documentation

Update `src/DurableTask.SqlServer/Scripts/README.md` for schema-development rules and `docs` for deployment-visible behavior. Call out rolling-upgrade or multitenancy implications whenever a database contract changes.
