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

#### Adding environment variables
Recidiviz depends on sensitive information to run. This data is stored in environment variables, which should be added
manually to your production environment (see `models/env_vars` for more information on the datastore kind used).

For local testing, these environment variables are loaded from `local.yaml` in your top-level project directory,
which is not provided in this repository. Instead, a template is provided (`local.example.yaml`) - run
`$ cp local.example.yaml local.yaml` to copy the template, then edit the new file to add values specific to your
project.

_Note: Recidiviz team members and partners can download a pre-populated `local.yaml` for local development - check your
onboarding document for details._

#### Running tests
Update your sourced `$PYTHONPATH` to add the Google App Engine libraries to the system Python path, which will
allow imports of the various GAE libraries to work in test mode. Add the following line to your shell profile:

`export PYTHONPATH="/path/to/google_appengine:/path/to/google_appengine/lib/:/path/to/google_appengine/lib/yaml-x.x/:$PYTHONPATH"`

If you installed the GCloud SDK via the steps above, it's probably wherever you chose to install the SDK, under
`google-cloud-sdk/platform/`. The `-x.x` after yaml is meant to denote a version number that will be present if you installed google_appengine as a separate component. If you did not, then it is unlikely that you will need to specify a version at all.

Finally, you will likely need to fix an [issue in the Google App Engine installation](https://stackoverflow.com/a/27274135)
that comes with the GCloud SDK. Check the `google_appengine/lib/fancy_urllib` folder to see if you have the nested
`__init__.py` files with an empty file on the outer layer. If so, follow the instructions in that answer, and maybe
copy the outer file into a temp file for safe-keeping (even though it's blank):

```
cd /path/to/google_appengine/lib/fancy_urllib
cp __init__.py old__init__.py
cp fancy_urllib/__init__.py __init__.py
```

Once that's all done, tests can be run from the root directory via `pytest`.

The configuration in `setup.cfg` and `.coveragerc` will ensure the right code is tested and the proper code coverage
metrics are displayed.

#### Checking code style
Run Pylint across the main body of code, in particular: `pylint *.py recidiviz`.

The output will include individual lines for all style violations, followed by a handful of reports, and finally a
general code score out of 10. Fix any new violations in your commit. If you believe there is cause for a rule change,
e.g. if you believe a particular rule is inappropriate in the codebase, then submit that change as part of your
inbound pull request.

### Running the app
There are two ways to run the app - on your local machine, or deployed to the cloud.

#### Local
Running from your local machine is preferred for development - it yields much quicker iteration cycles, and the local
dev server is able to handle the needs of the simple scraping tool pretty well.

To run this locally, just navigate to the directory you cloned pulse-data into and run `dev_appserver.py local_app.yaml`.

If you haven't run the local development server recently, or this is your first time running it at all, first make a
call to the test datastore populator (e.g., `localhost:8080/test_populator/clear`). Any call to the test data populator
(including `/clear`, which is used to empty the local datastore) will ensure the proper environment variables have been set
up for your local test system.

Logs will show up in the console you run the command in, and you can kick off the scraping by navigating in your browser
to `localhost:8080/scraper/start?region=[region]` (logs won't show much until the scraping starts). For now, use region `us_ny`

You can check datastore entries, queue tasks, and more on the local dev server admin page (http://localhost:8000/datastore)
while it's running.

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
