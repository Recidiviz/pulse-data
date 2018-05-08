# Recidiviz Data Platform

[![Build Status](https://travis-ci.org/Recidiviz/pulse-data.svg?branch=master)](https://travis-ci.org/Recidiviz/pulse-data) [![Coverage Status](https://coveralls.io/repos/github/Recidiviz/pulse-data/badge.svg?branch=master)](https://coveralls.io/github/Recidiviz/pulse-data?branch=master)

At the center of Recidiviz is our platform for tracking granular recidivism metrics in real time. It includes a system
for the ingest of corrections records from different criminal justice systems, and for calculation of recidivism metrics
from the ingested records.

Read more on data ingest in [`/scraper`](./scraper) and recidivism calculation in [`/calculator`](./calculator).

License
-------

This project is licensed under the terms of the GNU General Public License as published by the Free Software Foundation,
either version 3 of the License, or (at your option) any later version.

Data Access
------

The data that we have gathered from criminal justice systems has been sanitized, de-duplicated, and standardized in a
single schema. This processed data is central to our purposes but may be useful to others, as well. If you would like
access to the processed data, in whole or in part, please reach out to us at `team@recidiviz.com`. We evaluate such
requests on a case-by-case basis.

Calculated recidivism metrics can also be made available through the same process, though we anticipate publishing our
analysis in various forms and channels over time.

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

Then install project dependencies: `pip install -r requirements.txt`.

#### Running tests
Update your sourced `$PYTHONPATH` to add the Google App Engine libraries to the system Python path, which will
allow imports of the various GAE libraries to work in test mode. Add the following line to your shell profile:

`export PYTHONPATH="/path/to/google_appengine:/path/to/google_appengine/lib/:/path/to/google_appengine/lib/yaml/:$PYTHONPATH"`

If you installed the GCloud SDK via the steps above, it's probably wherever you chose to install the SDK, under
`google-cloud-sdk/platform/`.

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
Run Pylint across the main body of code, in particular: `pylint *.py calculator models scraper tests utils`.

The output will include individual lines for all style violations, followed by a handful of reports, and finally a
general code score out of 10. Fix any new violations in your commit. If you believe there is cause for a rule change,
e.g. if you believe a particular rule is inappropriate in the codebase, then submit that change as part of your
inbound pull request. 

### Running the app
There are two ways to run the app - on your local machine, or deployed to the cloud.

#### Local
Running from your local machine is preferred for development - it yields much quicker iteration cycles, and the local
dev server is able to handle the needs of the simple scraping tool pretty well.

To run this locally, just navigate to the directory you cloned pulse-data into and run `dev_appserver.py .`
(note the trailing dot).

Logs will show up in the console you run the command in, and you can kick off the scraping by navigating in your browser
to `localhost:8080/scraper/start?region=[region]` (logs won't show much until the scraping starts).

#### Production
To deploy to production AppEngine, navigate to the directory where you cloned pulse-data into and run
`gcloud app deploy`. This will upload the full project to the cloud and push it to production.

If it doesn't seem to know which project of yours to deploy to, or your account info, you may have skipped part of the
interactive setup for gcloud. Run `gcloud init` to revisit that setup.

Once the project is in production, you can kick off scraping by visiting `myproject.appspot.com/start`. You can monitor
the task queue (and purge it) in the Cloud Console, and read the service logs there as well.

**_(Note: Don't test in prod unless you really mean it! It will try to crawl production data systems at 1qps at the
moment, which may impact actual operations. We strongly recommend developing only with the local dev server, which you
can easily kill during tests with Ctrl+C.)_**
