### API Layer

This folder contains the HTTP-facing layer for the service.

- Defines FastAPI routers and routes.
- Maps HTTP requests and Pydantic DTOs to application-layer use-cases.
- Contains no business rules or infrastructure logic.

Recommended conventions:

- Keep request/response schemas in `schemas.py` or a dedicated `schemas/` subfolder.
- Group endpoints by resource or feature in separate router modules.
- Delegate all work to the `application` layer via dependencies.
