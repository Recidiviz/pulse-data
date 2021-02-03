# Running the app

## Backend
```bash
docker-compose -f docker-compose.case-triage.yaml up
````

We use `docker-compose` to run our development services locally, this includes:
- [`flask`](https://flask.palletsprojects.com/en/1.1.x/) web server
- [`postgres`](https://www.postgresql.org/) database
- `migrations` container, which automatically runs [`alembic`](https://alembic.sqlalchemy.org/) migrations


## Frontend
### Running
Install the dependencies by running `yarn` from this directory.
Run the frontend development server with `yarn run dev`.

To use a local version of `@recidiviz/case-triage-components` in conjunction with your development server,
follow these instructions: [Development alongside dependent projects](https://github.com/recidiviz/case-triage#2-development-alongside-dependent-projects)


This project was bootstrapped with [craco](https://github.com/gsoft-inc/craco).
