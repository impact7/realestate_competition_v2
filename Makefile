.PHONY: app-build
app-build:
	docker compose --env-file .env -f docker/app/docker-compose.yml build

.PHONY: app-clean-build
app-clean-build:
	docker compose --env-file .env -f docker/app/docker-compose.yml build --no-cache

.PHONY: app-up
app-up:
	docker compose --env-file .env -f docker/app/docker-compose.yml up

.PHONY: app-down
app-down:
	docker compose --env-file .env -f docker/app/docker-compose.yml down

.PHONY: app-run-pipeline-gcp
app-run-pipeline-gcp:
	docker compose --env-file .env -f docker/app/docker-compose.yml -f docker/app/docker-compose.override.yml \
 			run --rm ml_app python3 cli/cli_pipeline.py /app/json/pipeline/pipeline_gcp.json

.PHONY: app-run-pipeline92
app-run-pipeline92:
	docker compose --env-file .env -f docker/app/docker-compose.yml run --rm ml_app \
			python3 cli/cli_pipeline.py /app/json/pipeline/pipeline_rs92.json

.PHONY: app-run-pipeline-prepare
app-run-pipeline-prepare:
	docker compose --env-file .env -f docker/app/docker-compose.yml run --rm ml_app \
			python3 cli/prepare_data.py

.PHONY: infra-build
infra-build:
	docker compose --env-file .env -f docker/infra/docker-compose.yml build

.PHONY: infra-clean-build
infra-clean-build:
	docker compose --env-file .env -f docker/infra/docker-compose.yml build --no-cache

.PHONY: infra-up
infra-up:
	docker compose --env-file .env -f docker/infra/docker-compose.yml up

.PHONY: infra-down
infra-down:
	docker compose --env-file .env -f docker/infra/docker-compose.yml down
