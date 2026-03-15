### Domain Layer

This folder contains the core domain model of the service.

- Entities, aggregates, and value objects.
- Domain events capturing significant state changes.
- Pure business logic with no dependencies on frameworks or infrastructure.

The domain layer should not import from `api`, `application`, `infra`, or `observability`.

