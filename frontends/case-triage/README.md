# Running the app

## Backend

```bash
docker-compose up
```

We use `docker-compose` to run our development services locally, this includes:

- [`flask`](https://flask.palletsprojects.com/en/1.1.x/) web server
- [`postgres`](https://www.postgresql.org/) database
- `migrations` container, which automatically runs [`alembic`](https://alembic.sqlalchemy.org/) migrations

For those without access to GCR repository, you will have to build the image before being able to run docker-compose:

```bash
docker build . -t us.gcr.io/recidiviz-staging/appengine/default:latest
```

### Local Cloud Services

When running locally, we mock Google Cloud Storage and Google Secrets Manager to use the local filesystem.
Run this script from the root of the repository to set up the development secrets:

```bash
pushd recidiviz/case_triage
mkdir -p local/gcs/case-triage-data/ local/gsm/
echo $(python -c 'import uuid; print(uuid.uuid4().hex)') > local/gsm/case_triage_secret_key
echo $(gcloud secrets versions access latest --secret=case_triage_auth0 --project recidiviz-staging) > local/gsm/case_triage_auth0
echo '[{"email": "youremail@recidiviz.org", "is_admin": true}]' > local/gcs/case-triage-data/allowlist_v2.json
popd
```

## Frontend

### Running

Install the dependencies by running `yarn` from this directory.
Run the frontend development server with `yarn run dev`.

To use a local version of `@recidiviz/design-system` in conjunction with your development server,
follow these instructions: [Development alongside dependent projects](https://github.com/Recidiviz/web-libraries/tree/main/packages/design-system#2-development-alongside-dependent-projects)

This project was bootstrapped with [craco](https://github.com/gsoft-inc/craco).

## End-to-end tests

We have a suite of end-to-end tests built with [WebdriverIO](https://webdriver.io/docs/gettingstarted) and [Cucumber](https://cucumber.io/docs/cucumber/) that you can run.

### Preparing your test environment

We run the test suite against set of services that are separate from the development services. To start the test services, run:

```bash
docker-compose -f docker-compose.yaml -f docker-compose.case-triage.test.yaml up
```

You will need valid login credentials for the "dev" Auth0 tenant.

Create a file in this directory called `.env.e2e` to configure your local test environment:

```
TEST_AUTH_USER=<your email>
TEST_AUTH_PASSWORD=<your password>
# set this to false as needed; can help with debugging
RUN_TESTS_HEADLESS=true
```

With this file in place, you can run `yarn test:e2e`, which will run tests against `localhost:3000`. The test configuration is in `wdio.conf.js` and the tests themselves can be found in `src/cucumber`. Any arguments you pass to that command will be forwarded to the `wdio` CLI tool.
