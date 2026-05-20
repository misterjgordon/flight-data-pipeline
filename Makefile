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

# Deploy the bundle exactly as it was at the previous git commit (default: HEAD~1).
# Usage: make bundle-deploy-previous-git TARGET=dev
#        make bundle-deploy-previous-git TARGET=prod GIT_REV=abc123
.PHONY: bundle-deploy-previous-git
bundle-deploy-previous-git:
	@test -n "$(TARGET)" || (echo 'Usage: make bundle-deploy-previous-git TARGET=dev  (or prod)'; exit 1)
	@set -euo pipefail; \
	REV=$${GIT_REV:-$$(git rev-parse HEAD~1)}; \
	TMP=$$(mktemp -d); \
	trap 'rm -rf "$$TMP"' EXIT; \
	git archive --format=tar "$$REV" | tar -x -C "$$TMP"; \
	cd "$$TMP" && databricks bundle deploy --target "$(TARGET)"

# ── Pipeline ──────────────────────────────────────────────────────────────────

.PHONY: pipeline-start
pipeline-start:
	databricks bundle run flight_data_pipeline --target dev --no-wait

.PHONY: pipeline-start-prod
pipeline-start-prod:
	databricks bundle run flight_data_pipeline --target prod --no-wait

.PHONY: pipeline-stop
pipeline-stop:
	databricks pipelines stop flight_data_pipeline -t dev

.PHONY: pipeline-stop-prod
pipeline-stop-prod:
	databricks pipelines stop flight_data_pipeline -t prod

.PHONY: pipeline-status
pipeline-status:
	databricks bundle run --refresh flight_data_pipeline --target dev --dry-run 2>/dev/null || databricks bundle validate

# Full refresh: required after streaming→snapshot dataset-type changes or checkpoint issues.
.PHONY: pipeline-full-refresh-dev
pipeline-full-refresh-dev:
	databricks bundle run flight_data_pipeline --target dev --full-refresh-all --no-wait

.PHONY: pipeline-full-refresh-prod
pipeline-full-refresh-prod:
	databricks bundle run flight_data_pipeline --target prod --full-refresh-all --no-wait

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

# ── Tests ─────────────────────────────────────────────────────────────────────

.PHONY: test-unit
test-unit:
	uv run --extra dev pytest tests/unit -v

.PHONY: test-unit-contract
test-unit-contract:
	uv run --extra dev pytest tests/unit -m contract -v

# ── Help ──────────────────────────────────────────────────────────────────────

.PHONY: help
help:
	@echo ""
	@echo "Bundle"
	@echo "  make validate          Validate databricks.yml and all resources"
	@echo "  make deploy-dev        Deploy to dev  (workspace catalog)"
	@echo "  make deploy-prod       Deploy to prod (main catalog)"
	@echo "  make bundle-deploy-previous-git TARGET=dev   Deploy from prior commit (optional GIT_REV=sha)"
	@echo ""
	@echo "Pipeline"
	@echo "  make pipeline-start              Trigger a pipeline run (incremental)"
	@echo "  make pipeline-full-refresh-dev   Full refresh dev pipeline (reset checkpoints)"
	@echo "  make pipeline-full-refresh-prod Full refresh prod pipeline"
	@echo "  make pipeline-stop               Stop dev pipeline (bundle key)"
	@echo "  make pipeline-stop-prod          Stop prod pipeline"
	@echo "  make pipeline-status             Show current pipeline state"
	@echo ""
	@echo "Sync (dev hot-reload)"
	@echo "  make sync-pipeline     Watch + sync pipeline files to workspace"
	@echo "  make sync-notebooks    Watch + sync notebooks to workspace"
	@echo ""
	@echo "Auth"
	@echo "  make refresh-token     Refresh the Databricks MCP OAuth token"
	@echo ""
	@echo "Tests"
	@echo "  make test-unit         pytest tests/unit -v (skip reasons: default -rs in pyproject)"
	@echo "  make test-unit-contract  only @contract tests (no JVM)"
	@echo ""
