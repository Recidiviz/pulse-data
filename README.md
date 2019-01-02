# Recidiviz Data Platform

[![Build Status](https://travis-ci.org/Recidiviz/pulse-data.svg?branch=master)](https://travis-ci.org/Recidiviz/pulse-data) [![Coverage Status](https://coveralls.io/repos/github/Recidiviz/pulse-data/badge.svg?branch=master)](https://coveralls.io/github/Recidiviz/pulse-data?branch=master)

At the center of Recidiviz is our platform for tracking granular criminal justice metrics in real time. It includes a system
for the ingest of corrections records from different source data systems, and for calculation of various metrics from the
ingested records.

Read more on data ingest in [`/recidiviz/ingest`](./recidiviz/ingest) and calculation in [`/recidiviz/calculator`](./recidiviz/calculator).

License
-------

This project is licensed under the terms of the GNU General Public License as published by the Free Software Foundation,
either version 3 of the License, or (at your option) any later version.

Data Access
------

The data that we have gathered from criminal justice systems has been sanitized, de-duplicated, and standardized in a
single schema. This processed data is central to our purposes but may be useful to others, as well. If you would like
access to the processed data, in whole or in part, please reach out to us at `team@recidiviz.com`. We evaluate such
requests on a case-by-case basis, in conjunction with our partners.

Calculated metrics can also be made available through the same process, though we anticipate publishing our analysis
in various forms and channels over time.

Forking
------

The Recidiviz data system is provided as open source software - for transparency and collaborative development, to
help jump-start similar projects in other spaces, and to ensure continuation if Recidiviz itself ever becomes inactive.

If you plan to fork the project for work in the criminal justice space (to ingest from the same systems we are, or similar),
we ask that you first [contact us](mailto:team@recidiviz.com) for a quick consultation. We work carefully to ensure
that our scraping activities don't disrupt other users' experiences with the public data services we scrape, but if
multiple scrapers are running against the same systems, without knowing about one another, it may place excessive
strain on them and impact the services those systems provide.

If you have ideas or new work for the same data we're collecting, let us know and we'll work with you to find the
best way to get it done.

Development
------

### Why AppEngine?
AppEngine is both easy to rapidly build out services, and easy to scale up and down as needed. It also comes with
helpful utilities like [TaskQueues](https://cloud.google.com/appengine/docs/standard/python/taskqueue/push/) built-in,
so we don't have to worry about creating that functionality from scratch. Error handling is straight forward.

### Local Development

#### Getting set up
Install the GCloud SDK (we recommend using the [interactive installer](https://cloud.google.com/sdk/downloads#interactive)),
and clone the recidiviz repo from Github.

Your mileage may vary here. If you've installed via the interactive installer and cannot find `google_appengine` inside of
`google-cloud-sdk/platform/`, then you will need to install google_appengine separately by doing:
`gcloud components install app-engine-python`

Then install project dependencies with [`pipenv`](https://pipenv.readthedocs.io/en/latest/): `pipenv install`. Activate the environment with `pipenv shell`.

To generate a production environment `requirements.txt`, run: `pipenv lock --requirements > requirements.txt`

#### Adding secrets
Recidiviz depends on sensitive information to run. This data is stored in datastore, which should be added
manually to your production environment (see `utils/secrets` for more information on the datastore kind used).

For local testing, these secrets are loaded from `secrets.yaml` in your top-level project directory, which is not
provided in this repository. Instead, a template is provided (`secrets.example.yaml`) - run
`$ cp secrets.example.yaml secrets.yaml` to copy the template, then edit the new file to add values specific to your
project.

_Note: Recidiviz team members and partners can download a pre-populated `secrets.yaml` for local development - check
your onboarding document for details._

#### Running tests
Individual tests can be run via `pytest filename.py`. To run all tests, go to the root directory and run `pytest recidiviz`.

The configuration in `setup.cfg` and `.coveragerc` will ensure the right code is tested and the proper code coverage
metrics are displayed.

A few tests (such as `sessions.py`) depend on running emulators (i.e. [Cloud Datastore Emulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator)). These tests are skipped by default when run locally, but will always be tested by Travis. If you are modifying code tested by these tests then you can run the tests locally. You must first install the both emulators via `gcloud components install cloud-datastore-emulator` and `gcloud components install cloud-pusub-emulator`, which depends on the Java JRE (>=8). Then start the emulators and run the tests:

```bash
# Starts the emulator
$ gcloud beta emulators datastore start --no-store-on-disk --project test-project --consistency 1.0
$ gcloud beta emulators pubsub start --project test-project > ps_emulator.out 2> ps_emulator.err &
# Run the tests
$ pytest recidiviz --with-emulator
```

[A bug in the google client](https://github.com/googleapis/google-cloud-python/issues/5738) requires that you have default application credentials. This should not be necessary in the future. For now, make sure that you have done both `gcloud config set project recidiviz` and `gcloud auth application-default login`.

_Note: The emulator is a long running command, either (1) run it in a separate session or (2) run it in the background (suffix with `2> emulator.out &`) and bring it back with `fg`._

#### Checking code style
Run Pylint across the main body of code, in particular: `pylint *.py recidiviz`.

The output will include individual lines for all style violations, followed by a handful of reports, and finally a
general code score out of 10. Fix any new violations in your commit. If you believe there is cause for a rule change,
e.g. if you believe a particular rule is inappropriate in the codebase, then submit that change as part of your
inbound pull request.

### Running the app
There are two ways to run the app - on your local machine, or deployed to the cloud.

#### Local
A scraper can be run locally using the `run_scraper.py` script. See that file for instructions on how to run it.

By default the scraped entities will be logged. To persist data during a local run, set the `PERSIST_LOCALLY`
environment variable to `true`.

The full application can also be run locally using `flask run` and talk to the local emulators for GCP services (as
described in [running tests](#running-tests)). In practice, this is not particularly useful as there isn't a Cloud
Tasks emulator at this time. The [appengine documentation]( https://cloud.google.com/appengine/docs/standard/python3/testing-and-deploying-your-app)
has more information about running locally.

#### Deployed
To deploy to a live AppEngine project, navigate to the directory where you cloned `pulse-data` and run
`gcloud app deploy`. This will upload the full project to the cloud and enable it as the current version in your environment.

If you have multiple deployed projects, e.g. separate projects for staging and production, you can specify the project
for most `gcloud` commands like so: `gcloud app deploy --project=my-staging-env`

If it doesn't seem to know which project of yours to deploy to, or your account info, you may have skipped part of the
interactive setup for gcloud. Run `gcloud init` to revisit that setup.

Once the project is deployed, you can kick off scraping by visiting `myproject.appspot.com/start_scraper`. You can monitor
the task queue (and purge it) in the Cloud Console, and read the service logs there as well.

**_Note: Don't test in prod! A lot can go wrong (you could scrape in a way that doesn't throttle properly, you could
create data inconsistencies in our prod data, etc.), and at scale. We strongly recommend developing only with the local
dev server, which you can easily kill during tests with Ctrl+C.)_**
