# Recidiviz Data Platform

[![Coverage Status](https://coveralls.io/repos/github/Recidiviz/pulse-data/badge.svg?branch=master)](https://coveralls.io/github/Recidiviz/pulse-data?branch=master)

At the center of Recidiviz is our platform for tracking granular criminal justice metrics in real time. It includes a system
for the ingest of corrections records from different source data systems, and for calculation of various metrics from the
ingested records.

Read more on data ingest in [`/recidiviz/ingest`](./recidiviz/ingest) and calculation in [`/recidiviz/calculator`](./recidiviz/calculator).

## License

This project is licensed under the terms of the GNU General Public License as published by the Free Software Foundation,
either version 3 of the License, or (at your option) any later version.

## Data Access

The data that we have gathered from criminal justice systems has been sanitized, de-duplicated, and standardized in a
single schema. This processed data is central to our purposes but may be useful to others, as well. If you would like
access to the processed data, in whole or in part, please reach out to us at `team@recidiviz.com`. We evaluate such
requests on a case-by-case basis, in conjunction with our partners.

Calculated metrics can also be made available through the same process, though we anticipate publishing our analysis
in various forms and channels over time.

## Forking

The Recidiviz data system is provided as open source software - for transparency and collaborative development, to
help jump-start similar projects in other spaces, and to ensure continuity if Recidiviz itself ever becomes inactive.

If you plan to fork the project for work in the criminal justice space (to ingest from the same systems we are, or similar),
we ask that you first [contact us](mailto:team@recidiviz.org) for a quick consultation. We work carefully to ensure
that our ingest activities don't disrupt other users' experiences with the public data services we read, but if
multiple ingest processes are running against the same systems, without knowing about one another, it may place excessive
strain on them and impact the services those systems provide.

If you have ideas or new work for the same data we're collecting, let us know and we'll work with you to find the
best way to get it done.

## Development

If you are contributing to this repository regularly for an extended period of time, [request GitHub collaborator access](mailto:team@recidiviz.com?subject=GitHub%20collaborator%20request&body=) to commit directly to the main repository. If you are contributing on occasion, [fork this repository](https://github.com/Recidiviz/pulse-data/fork) before making any commits.

### Local Development

#### Environment setup

##### Option 1: Local Python installation

If you can install `python3.8` locally, do so. For local Python development, you will also need to install the `libpq` PostgreSQL client library and `openssl`.

On a Mac with [Homebrew](https://brew.sh/), you can install `python3.8` by first installing `pyenv` with:

```bash
brew install pyenv
brew install xz
mkdir ~/.pyenv
```

Then, add the following to your `~/.zshrc` (or equivalent):

```
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$HOME/.local/bin:$PATH"
if command -v pyenv 1>/dev/null 2>&1; then
eval "$(pyenv init -)"
fi
```

Then run:

```
pyenv install 3.8.8
pyenv global 3.8.8
```

Verify that you have the correct version of python across contexts by opening a new terminal window and running:

```
python -V
```

Once python is installed, you can install `libpq` and `openssl` with:

```bash
$ brew install postgresql openssl
```

On Ubuntu 18.04,`openssl` is installed by default, you can install `python3.8` and `libpq` with:

```bash
$ apt update -y && apt install -y python3.8-dev python3-pip libpq-dev
```

You do not need to change your default python version, as `pipenv` will look for 3.8.

Upgrade your `pip` to the latest version:

```bash
$ pip install -U pip
```

**NOTE**: if you get `ImportError: cannot import name 'main'` after upgrading
pip, follow the suggestions in
[this issue](https://github.com/pypa/pip/issues/5599).

If you do not already have `pip` installed, you can install it on a Mac with these commands:

```bash
$ curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
$ python get-pip.py --user
```

On Ubuntu 18.04, you can install `pip` with:

```
$ sudo apt-get install python-pip
```

Install [`pipenv`](https://pipenv.readthedocs.io/en/latest/):

```bash
$ pip install pipenv --user
```

[Fork this repository](https://github.com/Recidiviz/pulse-data/fork), clone it locally, and enter its directory:

```
$ git clone git@github.com:your_github_username/pulse-data.git
$ cd pulse-data
```

Create a new pipenv environment and install all project and development dependencies

On a Mac, run the `initial_pipenv_setup_mac` script.

**NOTE**: Installation of one of our dependencies (`psycopg2`) requires OpenSSL, and as OpenSSL is not linked on Macs by default, this script temporarily sets the necessary compiler flags and then runs `pipenv sync --dev`. After this initial installation all `pipenv sync/install`s should work without this script.

```bash
$ ./initial_pipenv_setup_mac.sh
```

On a Linux machine, run the following:

```bash
$ pipenv sync --dev
```

**NOTE**: if you get `pipenv: command not found`, add the binary directory to
your PATH as described
[here](https://pipenv.readthedocs.io/en/latest/install#pragmatic-installation-of-pipenv).

To activate your pipenv environment, run:

```bash
$ pipenv shell
```

Finally, run `pytest`. If no tests fail, you are ready to develop!

**NOTE**: If some `recidiviz/tests/ingest/aggregate` tests fail, you may need to install the Java Runtime Environment (JRE) version 7 or higher.

You can ignore those tests with:

```bash
$ pytest --ignore=recidiviz/tests/ingest/aggregate
```

On a Mac with [Homebrew](https://brew.sh/), you can install the JRE with:

```bash
$ brew cask install java
```

On Ubuntu 18.04, you can install the JRE with:

```bash
$ apt update -y && apt install -y default-jre
```

On a Mac with [Homebrew](https://brew.sh/), you can install jq (needed to deploy calculation pipelines) with:

```bash
$ brew install jq
```

On Ubuntu 18.04, you can install jq with:

```bash
$ apt update -y && apt install -y jq
```

##### Option 2: Docker container

If you can't install `python3.8` locally, you can use Docker instead.

Follow these instructions to install Docker on Linux:

- [Debian](https://docs.docker.com/install/linux/docker-ce/debian/#install-using-the-repository)
- [Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-using-the-repository)

Click the following links to directly download Docker installation binaries for Mac and Windows:

- [Mac](https://download.docker.com/mac/stable/Docker.dmg)
- [Windows](https://download.docker.com/win/stable/Docker%20for%20Windows%20Installer.exe)

Once Docker is installed, [fork this repository](https://github.com/Recidiviz/pulse-data/fork), clone it locally, and enter its directory:

```bash
$ git clone git@github.com:your_github_username/pulse-data.git
$ cd pulse-data
```

Build the image:

```bash
$ docker build -t recidiviz-image . --build-arg DEV_MODE=True
```

Stop and delete previous instances of the image if they exist:

```bash
$ docker stop recidiviz && docker rm recidiviz
```

Run a new instance, mounting the local working directory within the image:

```bash
$ docker run --name recidiviz -d -t -v $(pwd):/app recidiviz-image
```

Open a `bash` shell within the instance:

```bash
$ docker exec -it recidiviz bash
```

Once in the instance's `bash` shell, update your pipenv environment:

```bash
$ pipenv sync --dev
```

To activate your pipenv environment, run:

```bash
$ pipenv shell
```

Finally, run `pytest`. If no tests fail, you are ready to develop!

Using this Docker container, you can edit your local repository files and use `git` as usual within your local shell environment, but execute code and run tests within the Docker container's shell environment.

#### Google Cloud

Recidiviz interacts with Google Cloud services using [`google-cloud-*` Python client libraries](https://cloud.google.com/python/docs/reference).
During development, you may find it useful to verify the integration with these services.
First, [install the Google Cloud SDK](https://cloud.google.com/sdk/docs/install), then login to the SDK:

```bash
gcloud auth login # Gets credentials to interact with services via the CLI
gcloud auth application-default login # Gets credentials which will be automatically read by our client libraries
```

Lastly, in a test script, use the [`local_project_id_override` helper](https://github.com/Recidiviz/pulse-data/blob/c6972e132a6e68453e2d0baeb617b1f446a3e94f/recidiviz/utils/metadata.py#L69) to override configuration used by our client library wrappers:

```python
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.environment import GCP_PROJECT_STAGING

# Override configuration used by our client libraries
with local_project_id_override(GCP_PROJECT_STAGING):
    # Google Cloud Client libraries will use `recidiviz-staging` in this context
```

Now the code run in the above context will interact directly with our staging services. Use conservatively & exercise caution!

#### Adding secrets

Recidiviz depends on sensitive information to run. This data is stored in Cloud Datastore, which should be added
manually to your production environment (see `utils/secrets` for more information on the Datastore kind used).

#### Data model updates

The two main data models used in the system are the [`IngestInfo`](recidiviz/ingest/models/ingest_info.proto) object and the database [`schema`](recidiviz/persistence/database/schema), defined in various `schema.py` files.

When updating [`IngestInfo`](recidiviz/ingest/models/ingest_info.proto) be sure to update both the proto and the [python object](recidiviz/ingest/models/ingest_info.py). Then, re-generate the proto source: `protoc recidiviz/ingest/models/ingest_info.proto --python_out . --mypy_out .`. Be sure that `mypy-protobuf` is installed when compiling the protos. If it is not then pull it in from the dev packages using `pipenv install --dev`. If you do not have `protoc` installed, follow instructions to install [here](http://google.github.io/proto-lens/installing-protoc.html).

When updating any `schema.py` file in the [`schema`](recidiviz/persistence/database/schema), follow the directions on the [database wiki page](https://github.com/Recidiviz/pulse-data/wiki/Database) to create a schema migration.

When updating either, it may be necessary to update the [`converter`](recidiviz/persistence/ingest_info_converter/) code that handles converting between the two models.

#### Running tests

Individual tests can be run via `pytest filename.py`. To run all tests, go to the root directory and run `pytest recidiviz`.

The configuration in `setup.cfg` and `.coveragerc` will ensure the right code is tested and the proper code coverage
metrics are displayed.

A few tests (such as `sessions.py`) depend on running emulators (i.e. [Cloud Datastore Emulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator)). These tests are skipped by default when run locally, but will always be tested by Travis. If you are modifying code tested by these tests then you can run the tests locally. You must first install the both emulators via `gcloud components install cloud-datastore-emulator` and `gcloud components install pubsub-emulator`, which depends on the Java JRE (>=8). You will also need to install the beta command to execute these emulators, with `gcloud components install beta`. Then run the tests, telling it to bring up the emulators and include these tests:

```bash
$ pytest recidiviz --with-emulator
```

[A bug in the google client](https://github.com/googleapis/google-cloud-python/issues/5738) requires that you have
default application credentials. This should not be necessary in the future. For now, make sure that you have done
both `gcloud config set project recidiviz` and `gcloud auth application-default login`.

#### Checking code style

Run Pylint across the main body of code, in particular: `pylint recidiviz`.

The output will include individual lines for all style violations, followed by a handful of reports, and finally a
general code score out of 10. Fix any new violations in your commit. If you believe there is cause for a rule change,
e.g. if you believe a particular rule is inappropriate in the codebase, then submit that change as part of your
inbound pull request.

#### Autoformatting

We use `black` to ensure consistent formatting across the code base and `isort` to sort imports. There is a pre-commit hook that will format all of your files automatically. It is defined in `githooks/pre-commit` and is installed by `./initial_pipenv_setup_mac.sh`.

You can also set up your editor to run `black` and `isort` on save. See [the black docs](https://black.readthedocs.io/en/stable/integrations/editors.html) for how to configure external tools (both `black` and `isort`) to run in PyCharm (more info in PyCQA/isort#258).

In VSCode just add the following to your `.vscode/settings.json`:

```json
    "editor.formatOnSave": true,
    "python.formatting.provider": "black",
    "[python.editor.codeActionsOnSave]": {
        "source.organizeImports": true
    },
```

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
Tasks emulator at this time. The [appengine documentation](https://cloud.google.com/appengine/docs/standard/python3/testing-and-deploying-your-app)
has more information about running locally.

### Deployment

Install the GCloud SDK using the [interactive installer](https://cloud.google.com/sdk/docs/downloads-interactive).

Note: make sure the installer did not add `google-cloud-sdk/platform/google_appengine` or subdirectories thereof to your `$PYTHONPATH`, e.g. in your bash profile. This could break attempts to run tests within the `pipenv shell` by hijacking certain dependencies.

Make sure you have docker installed (see instructions above), then configure docker authentication:

```bash
$ gcloud auth login
$ gcloud auth configure-docker
```

### Deploying a scraper

The release engineer oncall should go through the following steps:

#### Push to staging

Typically on Monday morning the release engineer should:

1. Verify that the tests in `master` are all passing in [Travis](https://travis-ci.org/Recidiviz/pulse-data/branches).
1. Tag a commit with "va.b.c" following [semver](https://semver.org) for numbering. This will trigger a release to staging.
1. Once the release is complete, run [`https://recidiviz-staging.appspot.com/scraper/start?region=us_fl_martin`](https://recidiviz-staging.appspot.com/scraper/start?region=us_fl_martin) [TODO #623](https://github.com/Recidiviz/pulse-data/issues/623)
   and verify that it is happy by looking at the monitoring page [TODO #59](https://github.com/Recidiviz/pulse-data/issues/59) and also checking the logs for errors.
1. If it runs successfully, trigger a release to production by running `./deploy_production <release_tag>`

#### Push to prod

Typically on Wednesday morning the release engineer should:

1.  For every region that has `environment: staging` set, check the logs and monitoring in staging periodically to verify that they run successfully.
1.  For all regions that look good, set their environment to `production` and they will be ready to be deployed for the next week
1.  Be sure to file bugs/fixes for any errors that exist for other scrapers, and hold off on promoting them to production.

### Deploying a pipeline template

To deploy a pipeline job to a template in Cloud Storage without deploying the entire application, run the `deploy_pipeline_to_template.sh` script locally.
These jobs can then be run manually from the Dataflow interface using the "Create job from template" functionality.

### Troubleshooting

If you see a pipenv error (either during install or sync) with the following:

```
An error occurred while installing psycopg2==...
```

On a Mac:

1. Ensure `postgresql` and `openssl` are installed with: `brew install postgresql openssl`
2. Run the initial pipenv setup script: `./initial_pipenv_setup_mac.sh`

On Linux: Ensure `libpq` is installed with: `apt update -y && apt install -y libpq-dev`
