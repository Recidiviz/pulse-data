# Research Search

This service powers search over community resources (e.g., housing, healthcare, employment) using location, category, keyword, and semantic matching. 


### Initial setup

- Set up your backend developer environment (go/backend-eng-setup)

### Set up gcloud CLI

```
gcloud auth login
gcloud auth application-default login
gcloud config set project recidiviz-staging
gcloud auth configure-docker us-central1-docker.pkg.dev
```

### Development secrets

Application secrets are stored in Google Secrets Manager (GSM):

When running locally, we pull these secrets from GSM and write them to our local filesystem. The application code detects if we are in development, and if so, reads the secrets from our filesystem instead of GSM (see the `get_secret` method).

Run this script from the root of the repository (i.e. `pulse-data`) to set up the development secrets:

```bash
./recidiviz/tools/resource_search/initialize_development_environment.sh
```

### Build our Docker image

1. Build the base Docker image:

```
docker build . -f Dockerfile.recidiviz-base -t us-docker.pkg.dev/recidiviz-staging/appengine/default:latest --platform=linux/amd64
```

2. Build the resource search DB
```
docker compose build resource_search_db
```

## Running Locally

1. Make sure you've followed the steps above to build our Docker image.

2. Run the Justice Counts Docker image using the following command:

```bash
pipenv run docker-resource-search
```
