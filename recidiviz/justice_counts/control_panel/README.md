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
- [Testing playbook](https://docs.google.com/document/d/1893HOIq1X6rvcxI3UJfpmL7feub7ivgqaytI-Jyp7bE/edit)

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

1. Build the Recidiviz base Docker image:

```bash
docker build . -f Dockerfile.recidiviz-base -t us-central1-docker.pkg.dev/justice-counts-staging/recidiviz-base-images/recidiviz-base:latest --platform=linux/amd64 --build-arg DEV_MODE=True
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

### Debugging Locally

The VSCode debugger integrates with our locally-built docker container using Python's `debugpy` package.

This enables us to set breakpoints in the codebase which are acknowledged by a locally running Justice Counts flask app.

To enable the flask app to listen for VSCode breakpoints:

1. Start up a local instance of the JC app. You should see docker-compose logs indicating that the debugger can now be attached.

2. Set the desired breakpoints in VSCode.

3. Start a debug session by opening the `Run and Debug` tab and pressing the green `Start Debugging` arrow (or press `F5` as a shortcut).

Your local app should now be attached and listening to the VSCode debugger!

See [this walkthrough](https://www.loom.com/share/8c45a1f8632a449492287abf6ca1d9dc).

#### Attach Configuration

Note, you will need a launch.json attach configuration to tell the VS Code debugger how to connect to an app that is already running. Our Flask app is configured to listen to `0.0.0.0:5678`. Check your launch.json file for a configuration of type `attach` which points to `0.0.0.0:5678`.

The attach configuration should look like

```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python Debugger: Remote Attach",
            "type": "debugpy",
            "request": "attach",
            "connect": {
                "host": "0.0.0.0",
                "port": 5678
            },
            "pathMappings": [
                {
                    "localRoot": "${workspaceFolder}",
                    "remoteRoot": "/app"
                }
            ]
        }
    ]
}
```

If this configuration is not present, you can generate it using the following steps:

1. Open the Command Palette.
2. Select `Debug: Add Configuration...`.
3. In the dropdown, select `Python Debugger`, and then `Remote Attach`.
4. Enter `0.0.0.0` for the IP address.
5. Enter `5678` for the port number.
6. Modify the `remoteRoot` field from `"."` to `"/app"` (this is the Docker container's root).

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
3. [local] To run the migration against your local Postgres DB, run `pipenv run docker-jc`. During startup, it will apply the migration.
4. If you need to undo a local migration, find the revision of the migration right before yours, and run the following command, and then delete the autogenerated file.

```
docker exec -e SQLALCHEMY_DB_HOST=justice_counts_db -e SQLALCHEMY_DB_NAME=postgres -e SQLALCHEMY_USE_SSL=0 -e SQLALCHEMY_DB_USER=justice_counts_user -e SQLALCHEMY_DB_PASSWORD=example pulse-data-control_panel_backend-1  pipenv run alembic -c recidiviz/persistence/database/migrations/justice_counts_alembic.ini downgrade <revision>
```

5. [staging] To run the migration against the staging database, you will need to manually run the script below (with the dry-run flag set to False) after merging your PR to main. Make sure to run the script from an updated main branch that contains your db schema changes.

```
pipenv run python -m recidiviz.tools.migrations.run_migrations_to_head --database JUSTICE_COUNTS --project-id justice-counts-staging --dry-run
```

> Please note: You should only ever run migrations that are backwards compatible (i.e. safe to apply without any corresponding code changes). The database migration should always happen before the code that relies on the migration is merged. Deploying the code without first migrating the database will break our staging application.
> The best way to ensure a safe database migration is to follow these steps:
>
> - Separate out the schema change PR from the corresponding code changes (put them in separate PRs).
> - Put up the migration file for review first, merge it to main first, and then run the migration on the up-to-date main branch.
> - Only then should you merge the code changes that rely on that migration.

6. [prod] The migration will be applied to the prod database automatically during the next prod deploy.

Note: Migrations MUST be backwards compatible, otherwise staging will break in the duration between the migration and the subsequent staging deploy.

## Deployment

### Automatic Staging Deployment

Staging deployments are automatically triggered by the Continuous Staging Deploy Github Action every Monday - Thursday night at 3am EST/12am PST.

Sites for investigating deployment logs:

- [Github Action workflow logs](https://github.com/Recidiviz/pulse-data/actions/workflows/jc_continuous_staging_deploy.yml)
- [Cloud Build Trigger logs](https://console.cloud.google.com/cloud-build/builds;region=global?query=trigger_id%3D%2201e362f0-c3f3-40ac-b13a-c6881b3a272f%22&project=justice-counts-staging)

The Github Action will send a Slack notification to jc-eng-only with the deployment status, and will also raise an alert to the justice-counts Sentry project in case of failure.

If for some reason a manual staging deploy is required, use the deploy_to_staging.sh
script described in the `Deployment Scripts` section below.

For more documentation on our continuous deployment process, see [this page](https://www.notion.so/recidiviz/JC-Continuous-Staging-Deployment-ad9ea2a1c33d4d61b22280edb4843d02) or visit go/jc-staging-deployment-doc.

### Prod Deployment

Prod deployment is intentially manual - to ensure that we verify our staging
image is working as intended before deploying it to production. See the prod deployment
script in the `Deployment Scripts` section below.

### Manual Deployment Scripts

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

To get the version number of the latest docker image that was deployed to staging, run this script:

```
./recidiviz/tools/deploy/justice_counts/version_of_latest_staging_deploy.sh
```

### Deploy to Playtest using our Github Action

1. Click on the Actions tab in Github.

<img width="1183" alt="Screenshot 2024-02-08 at 10 03 48 AM" src="https://github.com/Recidiviz/pulse-data/assets/130382407/2861b26a-510e-4731-a3c2-285c3e4dbb7a">

2. Click on Deploy to JC Playtest on the left side bar.

3. Click on the Run workflow drop down on the right side.

<img width="1362" alt="Screenshot 2024-02-08 at 10 08 37 AM" src="https://github.com/Recidiviz/pulse-data/assets/130382407/5a7c5cae-dc67-44ae-af25-d46ddb6972c7">

4. Select the workflow parameters for the deployment:

- `Use workflow from`: The branch to deploy.
- `Service name`: The service to deploy to (either "publisher-web" or "agency-dashboard-web").
- `Playtest URL tag`: The tag for the playtest URL. This tag must be registered as an allowed URL in Auth0 under the Publisher application.

5. Select the green `Run workflow` to trigger the deploy.

6. The Recidiviz Helper bot will post a comment on the PR containing the playtesting link (or a failure report). Note that the playtest deploy may take up to 10 minutes after the bot posts the link.

Deploying to playtesting is available for both the backend `pulse-data` repository and the frontend `justice-counts` repository. If deploying from a `pulse-data` PR, the deployment uses the version of `justice-counts` stored on main. And if deploying from a `justice-counts` PR, the deployment uses the version of `pulse-data` stored on main.

## Common Issues and Solutions

#### _When running tests, I see the following pg_ctl error:_

```
E no data was returned by command ""/opt/homebrew/Cellar/postgresql@13/13.13/bin/postgres" -V"
E initdb: error: The program "postgres" is needed by initdb but was not found in the
E same directory as "/opt/homebrew/Cellar/postgresql@13/13.13/bin/initdb".
E Check your installation.
E pg_ctl: database system initialization failed
```

Try `brew reinstall postgresql@13`.

#### _I'm getting a GCP-related permissions error._

Try `gcloud auth application-default login`.

#### _When I try to run the backend locally, I see a ModuleNotFound error._

First, run `pipenv sync --dev` if you haven't already.

If that doesn't work, try rebuilding our Docker image following the steps [above](https://github.com/Recidiviz/pulse-data/tree/main/recidiviz/justice_counts/control_panel#build-our-docker-image).

#### _I tried to add a new package to our Pipfile, and now I see a ModuleNotFound error._

The issue might be that you added the package to the `[dev-packages]` section of the Pipfile, which means it won't get pulled into our production Docker images. If the package is needed in production, move it to the `[packages]` section. If the package is only needed when running locally, then make sure that the Docker image you're using was built with the flag `--build-arg DEV_MODE=True`.

#### _When I try to run the backend locally, I see an error on the `pulse-data-justice_counts_migrations-1` job_.

It's possible your migrations are out of sync. Try manually resetting via `update alembic_version set version_num = '<latest version>'.` You can find the latest migration version by looking in `recidiviz/persistence/database/migrations/justice_counts/versions/`.

If setting to the latest version doesn't work, try setting to the _second_ most recent version. This way the latest migration will be applied.

#### _When I try to run the frontend locally, I'm getting a `Failed to compile / Module not found` error._

Try `yarn` or `yarn install`.

#### _My frontend lint error isn't going away even though I ran yarn lint --fix._

`yarn lint --fix` will only apply to the current app folder you’re in. If you `cd ../common` and then run `yarn lint --fix` in there, it should take care of all of the lint errors!

#### _I deployed to playtesting but see an "Oops!, something went wrong" page._

You have to add your URL to Auth0 config. Log into Auth0, use the top left dropdown to switch to the recidiviz-justice-counts-staging tenant > click on Applications (left menu) > Applications > Publisher Single Page Application > then add the playtesting URL link to the following fields: Allowed Callback URLs, Allowed Logout URLs, Allowed Web Origins, and Allowed Origins (CORS).

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
