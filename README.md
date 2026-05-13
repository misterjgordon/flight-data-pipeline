# Flight Data Pipeline

A production-grade, end-to-end data engineering pipeline on Databricks — real-time global flight tracking from ingestion to interactive visualisation.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://flight-map-live.streamlit.app)

---

## Pipeline in action

![Lakeflow Declarative Pipeline DAG — completed run showing ingest_flights (streaming table, 7.4K records) → flights_cleaned (materialised view, 15K records, incremental) → top_countries, flights_stats, flight_origin, flights_map (all gold materialised views, green)](docs/screenshots/pipeline-dag.png)

*Live pipeline run in Databricks — streaming ingestion from OpenSky Network through incremental cleaning to four gold materialised views, all completing in under 15 seconds.*

---

## What this demonstrates

- **Real-time ingestion** — live ADS-B state vectors from the [OpenSky Network](https://opensky-network.org/) REST API
- **Declarative pipelines** — Lakeflow Spark Declarative Pipelines (DLT) with streaming tables and materialised views
- **Unity Catalog** — three-part naming, data governance, and lineage across dev/prod
- **Infrastructure as code** — Databricks Asset Bundles (`databricks.yml`) for reproducible, CI/CD-ready deploys
- **Environment separation** — `dev` deploys to personal workspace path; `prod` deploys to `/Workspace/Shared/`
- **Serverless compute** — no manual cluster management throughout
- **Full-stack app** — Streamlit app deployed to both Databricks Apps (authenticated) and Streamlit Community Cloud (public)

---

## Architecture

```mermaid
flowchart TD
    OpenSky["OpenSky Network API\n(live ADS-B feed)"]

    subgraph pipeline ["Lakeflow Declarative Pipeline"]
        ingest["ingest_flights\nStreaming table"]
        cleaned["flights_cleaned\nMaterialised view — incremental"]
        map["flights_map\nGold — map coordinates"]
        top["top_countries\nGold — leaderboard"]
        origin["flight_origin\nGold — % by country"]
        stats["flights_stats\nGold — aggregates"]

        ingest --> cleaned
        cleaned --> map
        cleaned --> top
        cleaned --> origin
        cleaned --> stats
    end

    subgraph catalog ["Unity Catalog (workspace.default.*)"]
        uc["Delta tables"]
    end

    map & top & origin & stats --> uc

    uc --> app1["Databricks Apps\n(authenticated)"]
    uc --> app2["Streamlit Community Cloud\nflight-map-live.streamlit.app"]

    OpenSky --> ingest
```

---

## Repo layout

```
databricks/
├── pipelines/
│   └── transformations/       # DLT pipeline — one file per table
│       ├── ingest_flights.py
│       ├── flights_cleaned.py
│       ├── flights_map.py
│       ├── top_countries.py
│       ├── flight_origin.py
│       └── flight_stats.py
├── apps/
│   └── flight_map/            # Streamlit app
│       ├── app.py
│       ├── app.yaml
│       └── requirements.txt
├── notebooks/                 # Exploratory / observability notebooks
├── resources/                 # DABs resource definitions (jobs, pipelines, app)
│   ├── flight_data_pipeline.yml
│   ├── flight_refresh_job.yml
│   ├── flight_explorer_job.yml
│   └── flight_map_app.yml
├── src/                       # Shared Python modules (importable)
├── tests/
│   ├── unit/                  # Local unit tests (no cluster required)
│   └── integration/           # Integration tests (require live cluster)
├── docs/
│   └── screenshots/           # Workspace screenshots for README
├── scripts/                   # Local CLI helpers
├── .env.example               # Required environment variables (template)
├── .streamlit/
│   └── secrets.toml.example   # Streamlit Cloud secrets template
├── databricks.yml             # Bundle root — targets dev + prod
└── Makefile                   # Developer workflow shortcuts
```

---

## Getting started

### Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) >= v0.292.0
- A Databricks workspace with Unity Catalog enabled
- Python 3.11+

### 1. Authenticate

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

### 2. Configure

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

Update `databricks.yml` with your workspace host.

### 3. Deploy to dev

```bash
make deploy-dev
```

This uploads all pipeline, notebook, and app files to your personal dev path and provisions all resources (pipeline, jobs, app).

### 4. Run the pipeline

```bash
make pipeline-start
```

### 5. Open the app

```bash
# After deploy, the app URL is printed — or find it with:
databricks apps list --profile DEFAULT
```

---

## Makefile reference

| Command | What it does |
|---------|-------------|
| `make validate` | Validate `databricks.yml` and all resource definitions |
| `make deploy-dev` | Deploy bundle to dev (personal workspace path) |
| `make deploy-prod` | Deploy bundle to prod (`/Workspace/Shared/`) |
| `make pipeline-start` | Trigger a pipeline refresh run |
| `make pipeline-stop` | Stop a running pipeline |
| `make sync-pipeline` | Hot-reload pipeline files to workspace (watch mode) |
| `make sync-notebooks` | Hot-reload notebooks to workspace (watch mode) |

---

## Running the Streamlit app locally

```bash
cd apps/flight_map
pip install -r requirements.txt
streamlit run app.py
```

Requires `DATABRICKS_HOST`, `STREAMLIT_CLOUD_READONLY_TOKEN`, and `DATABRICKS_WAREHOUSE_ID` in your `.env`. See `.env.example` and `.streamlit/secrets.toml.example`.

---

## Environment separation

| Target | Workspace path | Unity Catalog |
|--------|---------------|---------------|
| `dev` | `~/flight-data-pipeline/dev` | `workspace.default.*` |
| `prod` | `/Workspace/Shared/flight-data-pipeline/prod` | `main.default.*` |
