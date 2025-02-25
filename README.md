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

If you are contributing to this repository regularly for an extended period of time, [request GitHub collaborator access](mailto:team@recidiviz.com?subject=GitHub%20collaborator%20request&body=) to commit directly to the main repository. If you are contributing on occasion, [fork this repository](https://github.com/Recidiviz/pulse-data/fork) before making any commits.

### Local Development

#### Environment setup

##### Option 1: Local Python installation

If you can install `python3.7` locally, do so. 

On a Mac with [Homebrew](https://brew.sh/), you can install `python3.7` with:
```
brew install python3
```

On Ubuntu 18.04, you can install `python3.7` with:
```
apt update -y && apt install -y python3.7-dev python3-pip
```

You do not need to change your default python version, as `pipenv` will look for 3.7.

Upgrade your `pip` to the latest version:
```
pip install -U pip
```

Install [`pipenv`](https://pipenv.readthedocs.io/en/latest/):
```
pip install pipenv
```

[Fork this repository](https://github.com/Recidiviz/pulse-data/fork), clone it locally, and enter its directory: 
```
git clone git@github.com:your_github_username/pulse-data.git
cd pulse-data
```

Create a new pipenv environment and install all project and development dependencies:
```
pipenv sync --dev
```

To activate your pipenv environment, run:
```
pipenv shell
```

Finally, run `pytest`. If no tests fail, you are ready to develop!

**NOTE**: If some `recidiviz/tests/ingest/aggregate` tests fail, you may need to install the Java Runtime Environment (JRE) version 7 or higher.

You can ignore those tests with:
```
pytest --ignore=recidiviz/tests/ingest/aggregate
```

On a Mac with [Homebrew](https://brew.sh/), you can install the JRE with:
```
brew cask install java
```

On Ubuntu 18.04, you can install the JRE with:
```
apt update -y && apt install -y default-jre
```


##### Option 2: Docker container

If you can't install `python3.7` locally, you can use Docker instead.

Follow these instructions to install Docker on Linux:
* [Debian](https://docs.docker.com/install/linux/docker-ce/debian/#install-using-the-repository)
* [Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-using-the-repository)

Click the following links to directly download Docker installation binaries for Mac and Windows:
 * [Mac](https://download.docker.com/mac/stable/Docker.dmg)
 * [Windows](https://download.docker.com/win/stable/Docker%20for%20Windows%20Installer.exe)

Once Docker is installed, [fork this repository](https://github.com/Recidiviz/pulse-data/fork), clone it locally, and enter its directory: 
```
git clone git@github.com:your_github_username/pulse-data.git
cd pulse-data
```

Build the image:
```
docker build -t recidiviz-image . --build-arg DEV_MODE=True
```

Stop and delete previous instances of the image if they exist:
```
docker stop recidiviz && docker rm recidiviz
```

Run a new instance, mounting the local working directory within the image:
```
docker run --name recidiviz -d -t -v $(pwd):/app recidiviz-image
```

Open a `bash` shell within the instance:
```
docker exec -it recidiviz bash
```

Once in the instance's `bash` shell, update your pipenv environment:
```
pipenv sync --dev
```

To activate your pipenv environment, run:
```
pipenv shell
```

Finally, run `pytest`. If no tests fail, you are ready to develop! 

Using this Docker container, you can edit your local repository files and use `git` as usual within your local shell environment, but execute code and run tests within the Docker container's shell environment.


#### Adding secrets
Recidiviz depends on sensitive information to run. This data is stored in datastore, which should be added
manually to your production environment (see `utils/secrets` for more information on the datastore kind used).

For local testing, these secrets are loaded from `secrets.yaml` in your top-level project directory, which is not
provided in this repository. Instead, a template is provided (`secrets.example.yaml`) - run
`$ cp secrets.example.yaml secrets.yaml` to copy the template, then edit the new file to add values specific to your
project.

_Note: Recidiviz team members and partners can download a pre-populated `secrets.yaml` for local development - check
your onboarding document for details._

#### Data model updates
The two main data models used in the system are the [`IngestInfo`](recidiviz/ingest/models/ingest_info.proto) object and the database [`schema`](recidiviz/persistence/database/schema.py).

When updating [`IngestInfo`](recidiviz/ingest/models/ingest_info.proto) be sure to update both the proto and the [python object](recidiviz/ingest/models/ingest_info.py). Then, re-generate the proto source: `protoc recidiviz/ingest/models/ingest_info.proto --python_out . --mypy_out .`. Be sure that `mypy-protobuf` is installed when compiling the protos. If it is not then pull it in from the dev packages using `pipenv install --dev`.

When updating the [`schema`](recidiviz/persistence/database/schema.py) follow the directions in the [database readme](recidiviz/persistence/database/README.md#migrations) to create a schema migration.

When updating either, it may be necessary to update the [`converter`](recidiviz/persistence/converter/) code that handles converting between the two models.

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

#### Static type checking
Run Mypy across all code to check for static type errors: `mypy recidiviz`.

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

### Deployment

Install the GCloud SDK using the [interactive installer](https://cloud.google.com/sdk/docs/downloads-interactive).

Note: make sure the installer did not add `google-cloud-sdk/platform/google_appengine` or subdirectories thereof to your `$PYTHONPATH`, e.g. in your bash profile. This could break attempts to run tests within the `pipenv shell` by hijacking certain dependencies.   

### Deploying a scraper

The release engineer oncall should go through the following steps:

Note: The `queue.yaml` file is now generated using `python -m recidiviz.tools.build_queue_config`.

#### Push to staging
Typically on Monday morning the release engineer should:

1. Verify that the tests in `master` are all passing in [Travis](https://travis-ci.org/Recidiviz/pulse-data/branches).
1. Tag a commit with "va.b.c" following [semver](https://semver.org) for numbering. This will trigger a release to staging.
1. Once the release is complete, run `https://recidiviz-staging.appspot.com/scraper/start?region=us_fl_martin` [TODO #623](https://github.com/Recidiviz/pulse-data/issues/623)
and verify that it is happy by looking at the monitoring page [TODO #59](https://github.com/Recidiviz/pulse-data/issues/59) and also checking the logs for errors.
1. If it runs successfully, trigger a release to production by running `./deploy_production <release_tag>`

#### Push to prod
Typically on Wednesday morning the release engineer should:

1.  For every region that has `environment: staging` set, check the logs and monitoring in staging periodically to verify that they run successfully.
1.  For all regions that look good, set their environment to `production` and they will be ready to be deployed for the next week
1.  Be sure to file bugs/fixes for any errors that exist for other scrapers, and hold off on promoting them to production.
