# Prototypes Server

## Deploys

This project uses Google Cloud Build and Google Cloud Run to manage deployments for the prototypes server. Below are the options for deploying both to staging and production, as well as building and deploying Docker images locally.

### Staging Deploy

The Cloud Run trigger for staging deploys is located at `recidiviz/tools/deploy/prototypes/build-prototypes-server.yml`. The Cloud Build trigger, named `build-prototypes-server`, is configured in the `recidiviz-staging` GCP project.

To trigger a staging deploy, run the script `run_build_prototypes_server.py` within a `pipenv` shell:

```bash
# Deploy a specific version tag to staging
python -m recidiviz.tools.deploy.prototypes.run_build_prototypes_server \
    --version-tag {tag_name}

# Deploy a specific branch to staging
python -m recidiviz.tools.deploy.prototypes.run_build_prototypes_server \
    --version-branch {branch_name}
```

### Production Deploy

To deploy the latest staging version to production, use the `deploy_to_prod.sh` script, which moves the Docker image from `recidiviz-staging` to `recidiviz-123` and deploys it on Cloud Run.

```bash
./recidiviz/tools/deploy/prototypes/deploy_to_prod.sh
```

### Building and Deploying Locally

You can also build and deploy the Docker image locally using `pipenv` commands defined in the `Pipfile`:

```bash
# Build the base Recidiviz Docker image
pipenv run docker-build-base

# Build the prototypes Docker image
pipenv run docker-build-prototypes

# Deploy the prototypes image locally using Docker Compose
pipenv run docker-prototypes
```

The base Docker image (`Dockerfile.recidiviz-base`) is built first, and the prototypes image (`Dockerfile.prototypes`) builds on top of it. The `docker-compose` configuration (alias is `pipenv run docker-prototypes`) can be used to spin up the local prototypes service.

---

## Querying the Prototypes Endpoints

To interact with the prototypes endpoints, use the script located at `recidiviz/tools/prototypes/request_api.py`. This script allows you to compose queries and hit the endpoints.

### Prerequisites

1. Ensure that your local app is running by executing `docker-compose` via `pipenv run docker-prototypes`.
2. Run the prototypes initialization development script to get local secrets:

   ```bash
   ./recidiviz/tools/prototypes/initialize_development_environment.sh
   ```

# Case Note Search

This section details the functionality of the **Case Note Search** project hosted on the Prototypes server.

## Querying the Endpoint

To query the Case Note Search endpoint in any of the environments, you can use

```bash
# Query the dev environment
python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates", "user_id":"fake_id", "state_code":"US_ID"}' get --target_env dev

# Query the staging environment (requires token)
python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates", "user_id":"fake_id", "state_code":"US_ID"}' get --target_env staging --token {token}

# Query the production environment (requires token)
python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates", "user_id":"fake_id", "state_code":"US_ID"}' get --target_env prod --token {token}
```

The full list of required and optional parameters for the endpoint can be found in `recidiviz/prototypes/app.py` under search_case_notes().

### Temporary Token

The local API request uses an M2M auth token, so no token is required.

For staging and prod, we need to provide an acceptable token according to the
environment. To get a temporary staging/production token, you can log into the recidiviz
dashboard page, inspect the HTTP request packets, and copy-and-paste the token from the
Networks tab. See the screenshot below for the exact location.

![get-token-from-recidiviz-dashboard](https://github.com/user-attachments/assets/aab0e82b-7205-4345-acac-4d0db97dd9dc)
