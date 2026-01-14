.PHONY: help sync pylint docker-build docker-build-dev docker-build-base \
        cloudsql docker-admin docker-prototypes

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

uv-sync: ## Sync Python dependencies
	uv sync --all-extras

uv-shell: ## Instructions for how to enter a uv-shell
	@echo "The \`make\` tool cannot activate a virtual environment directly, because \`make\` runs commands in a subprocess that cannot modify your shell."
	@echo ""
	@echo "Run this command directly to activate your shell:"
	@echo "  source .venv/bin/activate"
	@echo ""
	@echo "To create a shortcut for activating the uv shell, add this to your ~/.zshrc:"
	@echo "  alias pulse-data-uv-shell='cd $(PWD) && source .venv/bin/activate'"

pylint: ## Run pylint
	./recidiviz/tools/lint/run_pylint.sh

docker-build: ## Build app Docker image
	docker build . -f Dockerfile --target recidiviz-app -t us-docker.pkg.dev/recidiviz-staging/appengine/default:latest --platform=linux/amd64

docker-build-dev: ## Build dev Docker image
	docker build . -f Dockerfile --target recidiviz-dev -t us-docker.pkg.dev/recidiviz-staging/appengine/default:latest --platform=linux/amd64

docker-build-base: ## Build base Docker image
	docker build . -f Dockerfile.recidiviz-base -t us-docker.pkg.dev/recidiviz-staging/recidiviz-base/default:latest --platform=linux/amd64

cloudsql: ## Access Cloud SQL instance
	./recidiviz/tools/postgres/access_cloudsql_instance.sh

docker-admin: ## Run admin panel locally
	docker compose -f docker-compose.yaml -f docker-compose.admin-panel.yaml up

docker-build-prototypes: ## Build prototypes Docker image
	docker build . -f Dockerfile.prototypes -t us-central1-docker.pkg.dev/recidiviz-staging/prototypes/main:latest --platform=linux/amd64

docker-prototypes: ## Run prototypes locally
	docker compose -f docker-compose.yaml -f docker-compose.prototypes.yaml up
