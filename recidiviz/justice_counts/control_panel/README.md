# Justice Counts Publisher: Backend

Welcome to the Justice Counts Publisher - a tool that allows agencies to report Justice Counts metrics.

The backend of this application, which lives in this directory, consists of a Python+Flask app. The frontend lives in the [justice-counts](https://github.com/Recidiviz/justice-counts) Github repo in the [`publisher`](https://github.com/Recidiviz/justice-counts/tree/main/publisher) directory.

To run the app locally, you need to spin up both the backend and frontend simultaneously. Instructions for spinning up the backend are below; instructions for spinning up the frontend can be found [here](https://github.com/Recidiviz/justice-counts/tree/main/publisher).

## Setting up your environment

### Aliases

See the [[scripts] section of our Pipfile](https://github.com/Recidiviz/pulse-data/blob/71d117466a7a1a07ed1dc0157bb0f8952abdd62d/Pipfile#L200) for some useful aliases, all of which can be run via `pipenv run <name>` (e.g. `pipenv run docker-jc`).

### Development secrets

The following application secrets are stored in Google Secrets Manager (GSM):

- `justice_counts_auth0`: A JSON blob containing Auth0 clientID, audience, domain, and algorithm
- `justice_counts_secret_key`: A Flask secret key used for securely signing the session cookie
- `justice_counts_segment_key`: Public key the backend serves to the control panel frontend to send analytics to the right Segment destination

When running locally, we pull these secrets from GSM and write them to our local filesystem. The application code detects if we are in development, and if so, reads the secrets from our filesystem instead of GSM (see the `get_secret` method).

Run this script from the root of the repository (i.e. `pulse-data`) to set up the development secrets:

```bash
./recidiviz/tools/justice_counts/control_panel/initialize_development_environment.sh
```

## Running locally

1. Build the Docker image using the command below: (At this point, it doesn't matter which frontend url you use, because you'll run the frontend locally in step 5, which will take precendence over the frontend that is bundled in the docker image.)

```bash
pipenv run docker-build-jc \
  --build-arg FRONTEND_URL=https://github.com/Recidiviz/justice-counts/archive/main.tar.gz \
  --build-arg FRONTEND_APP=publisher
```

2. Run `docker-compose`:

```bash
pipenv run docker-jc
```

We use `docker-compose` to run all services that the app depends on. This includes:

- [`flask`](https://flask.palletsprojects.com/en/1.1.x/) web server
- [`postgres`](https://www.postgresql.org/) database
- `migrations` container, which automatically runs the [`alembic`](https://alembic.sqlalchemy.org/) migrations for the Justice Counts database

3. (Optional) In another tab, while `docker-compose` is running, load fake data into your local database:

```bash
pipenv run fixtures-jc
```

4. In another tab, clone the [justice-counts](https://github.com/Recidiviz/justice-counts) repo and `cd` into the `publisher` directory.

5. Run `yarn install` and `yarn run dev`.

6. You should see the application running on `localhost:3000`!

## Connect to the local Postgres database

1. Look for `pulse-data_justice_counts_db_1` in your Docker dashboard, hover over it, and choose the CLI icon
2. In the terminal that opens, run `psql --dbname postgres -U justice_counts_user`

## Connect to the staging Postgres database

1. Run `brew install jq`.
2. From within `pulse-data`, run `pipenv run cloudsql`.
3. This will launch an interactive script. Select `recidiviz-staging` and then `justice-counts`. Select `yes` when asked if you want write access.
4. You should see a `postgres=>` prompt. Run `\dt` to see a list of tables.

## Deploying

Use the `./deploy_to_staging.sh`, `deploy_to_production.sh`, and `deploy_for_playtesting.sh` scripts in the `pulse-data/recidiviz/tools/deploy/justice_counts` directory.

## SQLAlchemy Primer

- A _session_ is a "holding zone" for all the objects you’ve loaded (via `session.query()`) or associated with it (via `session.add()`) during its lifespan.
- Until you (or a context manager) calls `session.commit()`, any changes you've made are _temporary_.
- Calling `session.commit()` will persist new data to the database, and also refresh objects with their new ID.
- If you're operating in a `with SessionFactory.using_database` context manager (i.e. in our unit tests), then `session.commit()` will be called ._automatically_ for you at the end of the block. Try to take advantage of this functionality and avoid calling `session.commit()` yourself in unit tests.
- In our API code, we don't the `with SessionFactory.using_database` context manager, so `session.commit()` will not be called for you automatically. Thus, at the end of any API method, you should explicitly call `current_session.commit()`.
- You generally shouldn't need to call `session.flush()` or `session.refresh()`. If you think you need to, add a comment explaining what was going wrong without it.
