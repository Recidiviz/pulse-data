# Justice Counts Control Panel: Backend

Welcome to the Justice Counts Control Panel - a tool that allows agencies to report Justice Counts metrics.

The backend of this application, which lives in this directory, consists of a Python+Flask app. The frontend lives in [pulse-data/frontends/justice-counts/control-panel](https://github.com/Recidiviz/pulse-data/tree/main/frontends/justice-counts/control-panel).

To run the app locally, you need to spin up both the backend and frontend simultaneously. Instructions for spinning up the backend are below; instructions for spinning up the frontend can be found in its directory's README.

## Setting up your environment

### Aliases

See the [scripts] section of our Pipfile for some useful aliases, all of which can be run via `pipenv run <name>` (e.g. `pipenv run docker-jc`).

### Development secrets

The following application secrets are stored in Google Secrets Manager (GSM):

- justice_counts_auth0: A JSON blob containing Auth0 clientID, audience, domain, and algorithm
- justice_counts_secret_key: A Flask secret key used for securely signing the session cookie
- justice_counts_segment_key: Public key the backend serves to the control panel frontend to send analytics to the right Segment destination

When running locally, we pull these secrets from GSM and write them to our local filesystem. The application code detects if we are in development, and if so, reads the secrets from our filesystem instead of GSM (see the `get_secret` method).

Run this script from the root of the repository (i.e. `pulse-data`) to set up the development secrets:

```bash
./recidiviz/tools/justice_counts/control_panel/initialize_development_environment.sh
```

## Running the backend

First build the Docker image with dev dependencies:

```bash
pipenv run docker-build-dev
```

Then run `docker-compose`:

```bash
docker-compose -f docker-compose.yaml -f docker-compose.justice-counts.yaml up
```

(Or equivalently, `pipenv run docker-jc`.)

We use `docker-compose` to run all services that the app depends on. This includes:

- [`flask`](https://flask.palletsprojects.com/en/1.1.x/) web server
- [`postgres`](https://www.postgresql.org/) database
- `migrations` container, which automatically runs the [`alembic`](https://alembic.sqlalchemy.org/) migrations for the Justice Counts database

## Testing end-to-end

1: Run the JC backend via docker:

```bash
pipenv run docker-jc
```

2: Load fixtures (fake data for testing):

````bash
pipenv run fixtures-jc

3: Run the JC frontend (`yarn run dev` from the `frontends/justice-counts/control-panel` directory). Login with your Recidiviz email address. You should see a message saying that the user is not connected to an agency.

4: Connect your user to an agency via the admin panel. Run the admin panel backend via docker:
```bash
pipenv run docker-admin
````

Then run the admin panel frontend (`yarn run dev` from the `frontends/admin-panel` directory). Go to the Agency Provisioning page in the left sidebar (scroll down) and connect your user to `Agency Alpha`.

5: Go back to the JC frontend and reload. You should see a report!

## SQLAlchemy Primer

- A _session_ is a "holding zone" for all the objects you’ve loaded (via `session.query()`) or associated with it (via `session.add()`) during its lifespan.
- Until you (or a context manager) calls `session.commit()`, any changes you've made are _temporary_.
- Calling `session.commit()` will persist new data to the database, and also refresh objects with their new ID.
- If you're operating in a `with SessionFactory.using_database` context manager (i.e. in our unit tests), then `session.commit()` will be called ._automatically_ for you at the end of the block. Try to take advantage of this functionality and avoid calling `session.commit()` yourself in unit tests.
- In our API code, `session.commit()` will not be called for you automatically. Thus, at the end of any ObjectInterface method that creates or updates objects, remember to call `session.add()` followed by `session.commit()`. You should call these methods in the Interface classes, not in the API itself.
- You generally shouldn't need to call `session.flush()` or `session.refresh()`. If you think you need to, add a comment explaining what was going wrong without it.

## Testing the backend API

1: Download `justice-counts-auth0-m2m-files.zip` from the Justice Counts 1password vault. Extract the contents (there should be two files) into the directory `recidiviz/pulse-data/recidiviz/justice_counts/control_panel/local/gsm`

2: Run docker:

```bash
pipenv run docker-jc
```

3: Load fixtures:

```bash
pipenv run fixtures-jc
```

4: Make test requests to the backend API:

```bash
# python -m recidiviz.tools.justice_counts.control_panel.request_api <endpoint name> <params>`
python -m recidiviz.tools.justice_counts.control_panel.request_api reports '{"user_id":0,"agency_id": 0}' get
python -m recidiviz.tools.justice_counts.control_panel.request_api users '{"email_address":"jsmith@gmail.com"}' post
python -m recidiviz.tools.justice_counts.control_panel.request_api reports '{"user_id":0,"agency_id": 0, "month": 3, "year": 2022, "frequency": "MONTHLY"}' post
```

Note that if you make changes to any of the fixtures .csv files, you'll have to re-run the `pipenv run fixtures-jc` script.

## Connect to the local postgres database

1: Look for `pulse-data_justice_counts_db_1` in your Docker dashboard, hover over it, and choose the CLI icon
2: In the terminal that opens, run `psql --dbname postgres -U justice_counts_user`
