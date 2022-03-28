# Justice Counts Control Panel: Backend

Welcome to the Justice Counts Control Panel - a tool that allows agencies to report Justice Counts metrics.

The backend of this application, which lives in this directory, consists of a Python+Flask app. The frontend lives in [pulse-data/frontends/justice-counts/control-panel](https://github.com/Recidiviz/pulse-data/tree/main/frontends/justice-counts/control-panel).

To run the app locally, you need to spin up both the backend and frontend simultaneously. Instructions for spinning up the backend are below; instructions for spinning up the frontend can be found in its directory's README.

## Setting up your environment

The following application secrets are stored in Google Secrets Manager (GSM):

- justice_counts_auth0: A JSON blob containing Auth0 clientID, audience, domain, and algorithm
- justice_counts_secret_key: A Flask secret key used for securely signing the session cookie

When running locally, we pull these secrets from GSM and write them to our local filesystem. The application code detects if we are in development, and if so, reads the secrets from our filesystem instead of GSM (see the `get_local_secret` method).

Run this script from the root of the repository (i.e. `pulse-data`) to set up the development secrets:

```bash
./recidiviz/tools/justice_counts/control_panel/initialize_development_environment.sh
```

## Running the backend

First build the Docker image:

```bash
docker build . -t us.gcr.io/recidiviz-staging/appengine/default:latest
```

Then run `docker-compose`:

```bash
docker-compose -f docker-compose.yaml -f docker-compose.justice-counts.yaml up
```

We use `docker-compose` to run all services that the app depends on. This includes:

- [`flask`](https://flask.palletsprojects.com/en/1.1.x/) web server
- [`postgres`](https://www.postgresql.org/) database
- `migrations` container, which automatically runs the [`alembic`](https://alembic.sqlalchemy.org/) migrations for the Justice Counts database
