PIPELINE_SRC := pipelines
PIPELINE_DEST := ~/flight-data-pipeline/dev/files/pipelines
NOTEBOOK_SRC := notebooks
NOTEBOOK_DEST := ~/flight-data-pipeline/dev/files/notebooks

# ── Bundle ────────────────────────────────────────────────────────────────────

.PHONY: validate
validate:
	databricks bundle validate

.PHONY: deploy-dev
deploy-dev:
	databricks bundle deploy --target dev

.PHONY: deploy-prod
deploy-prod:
	databricks bundle deploy --target prod

# ── Pipeline ──────────────────────────────────────────────────────────────────

.PHONY: pipeline-start
pipeline-start:
	databricks bundle run flight_data_pipeline --target dev --no-wait

.PHONY: pipeline-start-prod
pipeline-start-prod:
	databricks bundle run flight_data_pipeline --target prod --no-wait

.PHONY: pipeline-stop
pipeline-stop:
	databricks pipelines stop $(shell databricks bundle validate --output json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['resources']['pipelines']['flight_data_pipeline']['id'])" 2>/dev/null)

.PHONY: pipeline-status
pipeline-status:
	databricks bundle run --refresh flight_data_pipeline --target dev --dry-run 2>/dev/null || databricks bundle validate

# ── Sync (development hot-reload) ─────────────────────────────────────────────

.PHONY: sync-pipeline
sync-pipeline:
	databricks sync "$(PIPELINE_SRC)" "$(PIPELINE_DEST)" --watch

.PHONY: sync-notebooks
sync-notebooks:
	databricks sync "$(NOTEBOOK_SRC)" "$(NOTEBOOK_DEST)" --watch

# ── Auth ──────────────────────────────────────────────────────────────────────

.PHONY: refresh-token
refresh-token:
	scripts/refresh_mcp_token.sh

# ── Help ──────────────────────────────────────────────────────────────────────

.PHONY: help
help:
	@echo ""
	@echo "Bundle"
	@echo "  make validate          Validate databricks.yml and all resources"
	@echo "  make deploy-dev        Deploy to dev  (workspace catalog)"
	@echo "  make deploy-prod       Deploy to prod (main catalog)"
	@echo ""
	@echo "Pipeline"
	@echo "  make pipeline-start    Trigger a pipeline run"
	@echo "  make pipeline-stop     Stop a running pipeline"
	@echo "  make pipeline-status   Show current pipeline state"
	@echo ""
	@echo "Sync (dev hot-reload)"
	@echo "  make sync-pipeline     Watch + sync pipeline files to workspace"
	@echo "  make sync-notebooks    Watch + sync notebooks to workspace"
	@echo ""
	@echo "Auth"
	@echo "  make refresh-token     Refresh the Databricks MCP OAuth token"
	@echo ""
