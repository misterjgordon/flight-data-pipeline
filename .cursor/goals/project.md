# Project Goal

Build a production-grade, end-to-end ETL pipeline on Databricks that demonstrates
the full lifecycle of data engineering: extraction, transformation, loading, and
ongoing management — with every decision made at the standard of a senior engineer
working on a large-scale data warehouse.

## What this project must demonstrate

**Technical depth**
- Real-time data ingestion from an external API (OpenSky Network ADS-B feed)
- Streaming and batch transformations using Delta Live Tables
- Unity Catalog for data governance, lineage, and access control
- Databricks Asset Bundles for infrastructure-as-code deployment
- Environment separation (dev → prod) with isolated catalogs and schemas
- Serverless compute throughout (no manual cluster management)

**Engineering practices**
- Everything as code — no manual UI configuration, no one-off clicking
- CI/CD-ready bundle structure: `make deploy-dev` / `make deploy-prod`
- Clean repo layout that a new teammate can clone and run in minutes
- Proper use of DLT dependency tracking, materialized views, and streaming tables
- Notebooks for observability and data exploration, not for pipeline logic

**Scale readiness**
- Schema and catalog design that supports growth to additional pipelines and domains
- Artifact packaging (`src/`) for shared transformation logic as the codebase grows
- Workflow orchestration for multi-step pipeline runs and downstream jobs

## Standard to hold all work to

Every piece of code, configuration, and structure in this repo should be
defensible in a technical interview as "how I would do this on a real team."
If a shortcut is taken, it must be noted and the correct approach documented.

## Current pipeline

**Flight Data Pipeline** — ingests live global flight state vectors from the
OpenSky Network, transforms them into origin-country breakdowns and velocity
aggregates, and exposes them as Delta tables in Unity Catalog.

This pipeline is the foundation. Additional pipelines and domains will follow
the same bundle structure in separate repos.
