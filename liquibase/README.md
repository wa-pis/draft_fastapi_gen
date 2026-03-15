### Liquibase Migrations

This directory contains Liquibase changelogs for the platform's relational schema.

- `changelog-master.yaml` – master changelog that includes all service-specific changelog files.
- `migration/` – SQL and/or YAML changelogs for individual tables and features.

Use these changelogs to provision and evolve the PostgreSQL schema used by the services.

