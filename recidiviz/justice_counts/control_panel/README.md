# Justice Counts Control Panel

## Running the app

The Justice Counts Control Panel is a web app that consists of a Python+Flask backend and a React frontend. The backend lives in this directory and the frontend lives in pulse-data/frontends/justice-counts/control-panel. To run the app locally, you need to spin up both the backend and frontend simultaneously. Instructions for spinning up the backend are below; instructions for spinning up the frontend can be found in its directory.

## Backend

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
