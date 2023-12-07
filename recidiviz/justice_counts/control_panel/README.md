# Justice Counts Publisher: Backend

Welcome to the Justice Counts Publisher - a tool that allows agencies to report Justice Counts metrics.

The backend of this application, which lives in this directory, consists of a Python+Flask app. The frontend lives in the [justice-counts](https://github.com/Recidiviz/justice-counts) Github repo in the [`publisher`](https://github.com/Recidiviz/justice-counts/tree/main/publisher) directory.

To run the app locally, you need to spin up both the backend and frontend simultaneously. Instructions for spinning up the backend are below; instructions for spinning up the frontend can be found [here](https://github.com/Recidiviz/justice-counts/tree/main/publisher).

## Helpful Links

### Apps

- Publisher [[staging](https://publisher-staging.justice-counts.org/), [prod](https://publisher.justice-counts.org/)]
- Dashboard [[staging](https://dashboard-staging.justice-counts.org/), [prod](https://dashboard-demo.justice-counts.org/)]

### GCP Infra

- Publisher Cloud Run Service [[staging](https://console.cloud.google.com/run/detail/us-central1/justice-counts-web/revisions?project=recidiviz-staging)], [[prod](https://console.cloud.google.com/run/detail/us-central1/justice-counts-web/revisions?project=recidiviz-staging)]
- Dashboard Cloud Run Service [[staging](https://console.cloud.google.com/run/detail/us-central1/agency-dashboard-web/revisions?project=justice-counts-staging)], [[prod](https://console.cloud.google.com/run/detail/us-central1/agency-dashboard-web/revisions?project=justice-counts-production)]
- Cloud SQL database [[staging](https://console.cloud.google.com/sql/instances/dev-justice-counts-data/overview?project=justice-counts-staging), [prod](https://console.cloud.google.com/sql/instances?project=justice-counts-production)]
- Cloud Build Trigger [[staging](https://console.cloud.google.com/cloud-build/triggers?project=justice-counts-staging)]
- Cloud Run Jobs [[staging](https://console.cloud.google.com/run/jobs?project=justice-counts-staging), [prod](https://console.cloud.google.com/run/jobs?project=justice-counts-production)]
- Cloud Scheduler Jobs [[staging](https://console.cloud.google.com/cloudscheduler?referrer=search&project=justice-counts-staging), [prod](https://console.cloud.google.com/cloudscheduler?project=justice-counts-production)]
- Pub/Sub Topics [[staging](https://console.cloud.google.com/cloudpubsub/topic/list?project=justice-counts-staging), [prod](https://console.cloud.google.com/cloudpubsub/topic/list?referrer=search&project=justice-counts-production)]
- Secret Manager [[staging](https://console.cloud.google.com/security/secret-manager?project=justice-counts-staging), [prod](https://console.cloud.google.com/security/secret-manager?project=justice-counts-production)]

### Other

- Auth0 [[staging](https://manage.auth0.com/dashboard/us/recidiviz-justice-counts-staging/)], [[prod](https://manage.auth0.com/dashboard/us/recidiviz-justice-counts/)], [[configuration doc](https://paper.dropbox.com/doc/Justice-Counts-Auth0-Configuration--CDvUbVT17QfK99NI4HZCs3xBAg-qr7WeNZU8ISE8Ffta8oQi)]
- [Sentry](https://recidiviz-inc.sentry.io/issues/?project=4504532096516096&referrer=sidebar)
- [Oncall doc](go/jc-oncall)

## Setting up your environment

### Initial setup

- Set up your backend developer environment (go/backend-eng-setup)
- Set up your [frontend developer environment](https://docs.google.com/document/d/1y-yJwZN6yM1s5OKqTDCk56FN2p7ZA62buwph1YdnJAc)

### Set up gcloud CLI

```
gcloud auth login
gcloud auth application-default login
gcloud config set project recidiviz-staging
gcloud auth configure-docker us-central1-docker.pkg.dev
```

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

### Aliases

See the [[scripts] section of our Pipfile](https://github.com/Recidiviz/pulse-data/blob/71d117466a7a1a07ed1dc0157bb0f8952abdd62d/Pipfile#L200) for some useful aliases, all of which can be run via `pipenv run <name>` (e.g. `pipenv run docker-jc`).

### Build our Docker image

1. Pull the Recidiviz base Docker image:

```bash
docker pull us-central1-docker.pkg.dev/justice-counts-staging/recidiviz-base-images/recidiviz-base:latest
```

2. Build the Justice Counts Docker image using the command below: (At this point, it doesn't matter which frontend url you use, because you'll run the frontend locally, which will take precedence over the frontend that is bundled in the docker image.)

```bash
pipenv run docker-build-jc \
  --build-arg FRONTEND_URL=https://github.com/Recidiviz/justice-counts/archive/main.tar.gz
```

NOTE: If you get a 401 Unauthorized permissions error, run `gcloud auth configure-docker us-central1-docker.pkg.dev` and then retry.

## Running Locally

1. Make sure you've followed the steps above to build our Docker image.

2. Run the Justice Counts Docker image using `docker-compose`:

```bash
pipenv run docker-jc
```

We use `docker-compose` to run all services that the app depends on. This includes:

- [`flask`](https://flask.palletsprojects.com/en/1.1.x/) web server
- [`postgres`](https://www.postgresql.org/) database
- `migrations` container, which automatically runs the [`alembic`](https://alembic.sqlalchemy.org/) migrations for the Justice Counts database

3. [Only needs to be done once] In another tab, while `docker-compose` is running, load fake data into your local database:

```bash
pipenv run fixtures-jc
```

If this errors with "No such container", try:

```bash
docker exec <name of your Docker container> pipenv run python -m recidiviz.tools.justice_counts.control_panel.load_fixtures
```

4. In another tab, clone the [justice-counts](https://github.com/Recidiviz/justice-counts) repo and `cd` into the `publisher` directory.

5. [Only needs to be done once] Run `cp .env.example .env` and `yarn install`. (Note: the `.env` file determines which backend the frontend will run against. It defaults to your local backend, which you started running via `docker-compose`. If you want the frontend to use a staging backend, adjust the file to point to the corresponding URL.)

6. Run `yarn run dev`

7. You should see the application running on `localhost:3000`!

### Help! I'm seeing an error

Try rebuilding our Docker image:

```
docker pull us-central1-docker.pkg.dev/justice-counts-staging/recidiviz-base-images/recidiviz-base:latest
pipenv run docker-build-jc \
  --build-arg FRONTEND_URL=https://github.com/Recidiviz/justice-counts/archive/main.tar.gz
```

If you're adding a new dependency, you'll also need to rebuild the base image locally:

```
docker build . -f Dockerfile.recidiviz-base -t us-central1-docker.pkg.dev/justice-counts-staging/recidiviz-base-images/recidiviz-base:latest --platform=linux/amd64
pipenv run docker-build-jc \
  --build-arg FRONTEND_URL=https://github.com/Recidiviz/justice-counts/archive/main.tar.gz
```

## Databases

### Connect to the local Postgres database

1. Once you have the Justice Counts Docker image running locally, look for `justice_counts_db_1` in your Docker dashboard, click on 'Show Container Actions' (3 dots on the right side), and click 'Open in Terminal'.
2. In the terminal that opens, run `psql --dbname postgres -U justice_counts_user`
3. You should now be able to type psql commands and interact directly with your local database! For example, the `\dt` command will show all of the tables within the database.

### Connect to the staging or production Postgres database

1. Run `brew install jq` (this only has to be done once).
2. From within `pulse-data`, run `pipenv run cloudsql`.
3. This will launch an interactive script.\
   a. To connect to the staging database select `justice-counts-staging`\
   b. To connect to the production database select `justice-counts-production`
4. Next, select `justice_counts_v2`.
5. Select `y` when asked if you want write access.
6. You should see a `postgres=>` prompt. Run `\dt` to see a list of tables.
7. To exit the database, type 'quit' and enter.

### Example SQL Queries

The following query pulls all existing report datapoints for a given agency and copies the output to a csv file.

```
\copy (select * from datapoint join (select * from report where source_id=<SOURCE_ID>) as reports on datapoint.report_id=reports.id) to '/Users/<USERNAME>/Recidiviz/pulse-data/<SOURCE_NAME>_datapoints.csv' csv header;

```

### Updating DB Schema

If you need to update our DB schema (add a table or column, remove a column, change a column, etc) then you need to run an Alembic migration. The steps are as follows:

1. First make the change to the schema in recidiviz/persistence/database/schema/justice_counts/schema.py.
2. Run `pipenv run migrate-jc "name of migration"`. This will autogenerate a migration file!
3. To run the migration against your local Postgres DB, run `pipenv run docker-jc`. During startup, it will apply the migration.
4. If you need to undo a local migration, find the revision of the migration right before yours, and run the following command, and then delete the autogenerated file.

```
docker exec -e SQLALCHEMY_DB_HOST=justice_counts_db -e SQLALCHEMY_DB_NAME=postgres -e SQLALCHEMY_USE_SSL=0 -e SQLALCHEMY_DB_USER=justice_counts_user -e SQLALCHEMY_DB_PASSWORD=example pulse-data_control_panel_backend-1  pipenv run alembic -c recidiviz/persistence/database/migrations/justice_counts_alembic.ini downgrade <revision>
```

5. To run the migration against our staging or prod databases, commit your PR. It will get applied in the next deploy.

## Deploying

Pre-deploy:

1. Make sure you've run `brew install jq` (this only has to be done once)
2. Make sure your Docker daemon is running

Use the `./deploy_to_staging.sh`, `deploy_to_production.sh`, and `deploy_for_playtesting.sh` scripts in the `pulse-data/recidiviz/tools/deploy/justice_counts` directory. Example usages:

```
./recidiviz/tools/deploy/justice_counts/deploy_for_playtesting.sh -b <name of backend branch> -f <name of frontend branch> -a publisher -t playtesting

./recidiviz/tools/deploy/justice_counts/deploy_to_staging.sh

./recidiviz/tools/deploy/justice_counts/deploy_to_prod.sh -v v1.0.0
```

To determine what commits will be included in a deploy to production, run the following, passing in the version of the staging deploy that is the upcoming production candidate:

```
./recidiviz/tools/deploy/justice_counts/commits_to_be_deployed.sh -v v1.0.0
```

## Creating Users and Agencies

1. Visit the Admin Panel at [go/admin](https://recidiviz-staging.ue.r.appspot.com/admin/justice_counts_tools/agency_provisioning) (staging) or [go/admin-prod](https://recidiviz-123.ue.r.appspot.com/admin/justice_counts_tools/agency_provisioning) (production)
2. Navigate to the Justice Counts tab
3. To create a new Agency, navigate to the Agency Provisioning page and scroll to "Add Agency"
   1. Enter the Agency Name, System(s), State, and (optional) County (You may be able to infer the agency's system / state / county from its name + a Google search; if not, ask CSG)
   2. Click Submit
   3. The Agency should now appear in the Agency Provisioning table (this table is searchable)
4. To create a new User, navigate to the User Provisioning page and scroll to "Add User"
   1. Enter the User's Name and Email (You may be able to infer the user's name from their email; if not, ask CSG)
   2. Click Submit
   3. The User should now appear in the User Provisioning table (this table is searchable)
5. You can add/remove a user to an agency either from the User Provisioning table ("Agencies" column) or the Agency Provisioning table ("Team Members" column).
6. To update a user's role for a given agency, navigate to the Agency Provisioning page
   1. Find the given agency in the Agency Provisioning table
   2. Click the "Team Member Roles" link
   3. Find the given user in the Agency Team Members table
   4. Within the user's row, there should be a drop-down to specify their role (select JUSTICE_COUNTS_ADMIN, AGENCY_ADMIN, or CONTRIBUTOR)

## Using Jupyter Notebooks

Jupyter notebooks are a great way to interact with our code and database.

1. Git clone the [recidiviz-research](https://github.com/Recidiviz/recidiviz-research) repository
2. Follow the [Environment Setup instructions](https://github.com/Recidiviz/recidiviz-research#environment-setup)
3. Run `jupyter-notebook`, which should open a file directory in your browser
4. Navigate to `shared/justice-counts` and open `justice-counts-template.ipynb`
5. Uncomment the print statements in the last two cells
6. Run the notebook via Cell > Run All. You should see a list of all agencies printed twice at the end!

## SQLAlchemy Primer

- A _session_ is a "holding zone" for all the objects you’ve loaded (via `session.query()`) or associated with it (via `session.add()`) during its lifespan.
- Until you (or a context manager) calls `session.commit()`, any changes you've made are _temporary_.
- Calling `session.commit()` will persist new data to the database, and also refresh objects with their new ID.
- If you're operating in a `with SessionFactory.using_database` or `SessionFactory.for_proxy` context manager (e.g. in our unit tests or scripts), then `session.commit()` will be called ._automatically_ for you at the end of the block. Try to take advantage of this functionality and avoid calling `session.commit()` yourself in unit tests.
- In our API code, we don't the `with SessionFactory.using_database` context manager, so `session.commit()` will not be called for you automatically. Thus, at the end of any API method, you should explicitly call `current_session.commit()`.
- You generally shouldn't need to call `session.flush()` or `session.refresh()`. If you think you need to, add a comment explaining what was going wrong without it.
