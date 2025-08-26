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
docker compose -f docker-compose.resource-search.yaml build resource_search_db
```

3. Start the local PostgreSQL database:

```bash
pipenv run docker-resource-search
```

This command starts both the PostgreSQL database container (`resource_search_db`) and runs any pending migrations. The database will be available on port 5440.

### Running Migrations

To create and apply database schema changes, use the Alembic migration system:

After making schema changes, run:
```bash
pipenv run migrate-resource-search --message "description_of_changes"
```

### Migration Troubleshooting

If you encounter PostgreSQL version conflicts or connection issues:

1. **Clean up existing containers and volumes**:
```bash
docker compose -f docker-compose.resource-search.yaml down -v
```

2. **Restart the database**:
```bash
pipenv run docker-resource-search
```

The `-v` flag removes volumes, which clears any existing database data that might be incompatible with the current PostgreSQL version.

## Databases

### Connect to the local Postgres database

1. Once you have the Resource Search Docker image running locally, look for `resource_search_db-1` in your Docker dashboard, click on 'Exec'.
2. In the window that opens, run `psql --dbname postgres -U resource_user`
3. You should now be able to type psql commands and interact directly with your local database! For example, the `\dt` command will show all of the tables within the database.

Alternatively, you can connect from your local machine using:
```bash
psql -h localhost -p 5440 -U resource_user -d postgres
```

## Running Locally

1. Make sure you've followed the steps above to build our Docker image.

2. Start the local database:

```bash
pipenv run docker-resource-search
```

3. Run the webscraping job with:

```bash
pipenv run scrape-resources
```
