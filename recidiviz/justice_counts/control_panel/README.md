# Justice Counts Publisher: Backend

Welcome to the Justice Counts Publisher - a tool that allows agencies to report Justice Counts metrics.

The backend of this application, which lives in this directory, consists of a Python+Flask app. The frontend lives in the [justice-counts](https://github.com/Recidiviz/justice-counts) Github repo in the [`publisher`](https://github.com/Recidiviz/justice-counts/tree/main/publisher) directory.

To run the app locally, you need to spin up both the backend and frontend simultaneously. Instructions for spinning up the backend are below; instructions for spinning up the frontend can be found [here](https://github.com/Recidiviz/justice-counts/tree/main/publisher).

## Helpful Links

- GCP Cloud Run [[staging](https://console.cloud.google.com/run/detail/us-central1/justice-counts-web/revisions?project=recidiviz-staging)]
- GCP Gloud Run [[prod](https://console.cloud.google.com/run/detail/us-central1/justice-counts-web/revisions?project=recidiviz-staging)]
- Auth0 [[staging](https://manage.auth0.com/dashboard/us/recidiviz-justice-counts-staging/)]
- Auth0 [[prod](https://manage.auth0.com/dashboard/us/recidiviz-justice-counts/)]
- [Sentry](https://recidiviz-inc.sentry.io/issues/?project=4504532096516096&referrer=sidebar)
- [Oncall doc](go/jc-oncall)

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

1. Build the Justice Counts Docker image using the command below: (At this point, it doesn't matter which frontend url you use, because you'll run the frontend locally in step 7, which will take precendence over the frontend that is bundled in the docker image.)

```bash
pipenv run docker-build-jc-publisher \
  --build-arg FRONTEND_URL=https://github.com/Recidiviz/justice-counts/archive/main.tar.gz \
```

2. Now run the Justice Counts Docker image using `docker-compose`:

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

If this errors with "No such container", try:

```bash
docker exec <name of your Docker container> pipenv run python -m recidiviz.tools.justice_counts.control_panel.load_fixtures
```

4. In another tab, clone the [justice-counts](https://github.com/Recidiviz/justice-counts) repo and `cd` into the `publisher` directory.

5. Run `yarn install`, `cp .env.example .env`, and `yarn run dev`. (Note: the `.env` file determines which backend the frontend will run against. It defaults to your local backend, which you started running via `docker-compose`. If you want the frontend to use a staging backend, adjust the file to point to the corresponding URL.)

6. You should see the application running on `localhost:3000`!

## Databases

### Connect to the local Postgres database

1. Look for `pulse-data_justice_counts_db_1` in your Docker dashboard, hover over it, and choose the CLI icon
2. In the terminal that opens, run `psql --dbname postgres -U justice_counts_user`

### Connect to the staging Postgres database

1. Run `brew install jq`.
2. From within `pulse-data`, run `pipenv run cloudsql`.
3. This will launch an interactive script. Select `recidiviz-staging` and then `justice-counts`. Select `yes` when asked if you want write access.
4. You should see a `postgres=>` prompt. Run `\dt` to see a list of tables.

### Example SQL Queries

The following query pulls all existing report datapoints for a given agency and copies the output to a csv file.

```
\copy (select * from datapoint join (select * from report where source_id=<SOURCE_ID>) as reports on datapoint.report_id=reports.id) to '/Users/<USERNAME>/Recidiviz/pulse-data/<SOURCE_NAME>_datapoints.csv' csv header;

```

## Deploying

Use the `./deploy_to_staging.sh`, `deploy_to_production.sh`, and `deploy_for_playtesting.sh` scripts in the `pulse-data/recidiviz/tools/deploy/justice_counts` directory. Example usages:

```
./recidiviz/tools/deploy/justice_counts/deploy_for_playtesting.sh -b <name of backend branch> -f <name of frontend branch> -a publisher -t playtesting

./recidiviz/tools/deploy/justice_counts/deploy_to_staging.sh -a publisher

./recidiviz/tools/deploy/justice_counts/deploy_to_prod.sh -b v1.0.0 -f v1.0.0 -a publisher
```

The versions we've deployed to prod are tracked [here](https://paper.dropbox.com/doc/Justice-Counts-Deploy-Log--ByP1W00sWJCrBjelmL9_TVD_Ag-rxYzPkYw2yDQzobnXYM3f).

## Creating Users and Agencies

- To create a new Agency, use the Justice Counts > Agency Provisioning page in the admin panel (go/admin or go/admin-prod).
- To add or remove an _existing_ user from an agency, you can use the Justice Counts > User Provisioning page in the admin panel.
- To create a _new_ user, you must create them in both Auth0 and in our database. After creating them in Auth0, you can use the pulse-data/recidiviz/tools/justice_counts/create_user.py script to create a corresponding user in our DB.
- To update a user's role for a given agency, you can use the pulse-data/recidiviz/tools/justice_counts/update_user_role.py script (this can also be used to add a user to an agency)

## SQLAlchemy Primer

- A _session_ is a "holding zone" for all the objects you’ve loaded (via `session.query()`) or associated with it (via `session.add()`) during its lifespan.
- Until you (or a context manager) calls `session.commit()`, any changes you've made are _temporary_.
- Calling `session.commit()` will persist new data to the database, and also refresh objects with their new ID.
- If you're operating in a `with SessionFactory.using_database` context manager (i.e. in our unit tests), then `session.commit()` will be called ._automatically_ for you at the end of the block. Try to take advantage of this functionality and avoid calling `session.commit()` yourself in unit tests.
- In our API code, we don't the `with SessionFactory.using_database` context manager, so `session.commit()` will not be called for you automatically. Thus, at the end of any API method, you should explicitly call `current_session.commit()`.
- You generally shouldn't need to call `session.flush()` or `session.refresh()`. If you think you need to, add a comment explaining what was going wrong without it.
