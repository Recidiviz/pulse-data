# Running the app

## Backend

```bash
docker-compose -f docker-compose.case-triage.yaml up
```

We use `docker-compose` to run our development services locally, this includes:

- [`flask`](https://flask.palletsprojects.com/en/1.1.x/) web server
- [`postgres`](https://www.postgresql.org/) database
- `migrations` container, which automatically runs [`alembic`](https://alembic.sqlalchemy.org/) migrations

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
