# Recidiviz Airflow and Cloud Composer Configuration

## High Level Overview

[Cloud Composer](https://cloud.google.com/composer/docs) is Google Cloud's fully managed 
environment for [Apache Airflow](https://airflow.apache.org/), in which we can upload the
source code of defined Direct Acyclic Graphs (DAGs) that allow us to orchestrate certain processes
to Google Cloud Storage, in turn allowing us to deploy them across our Google Cloud
infrastructure. 

## Important Links

* Google Cloud Composer
    * Staging: https://console.cloud.google.com/composer/environments?project=recidiviz-staging
    * Prod: https://console.cloud.google.com/composer/environments?project=recidiviz-123
* Airflow UI
    * http://go/airflow-staging
    * http://go/airflow-prod

Each Cloud Composer environment has an associated Google Cloud Storage bucket that holds
the source code for all of the DAGs deployed to that environment. For our orchestration
environments, we've also defined the configuration in Terraform (see `cloud-composer.tf`).

The Airflow UI provides a way to generally trigger DAGs and to see how tasks are dependent
on each other and also how long they generally take. Both the Graph view and the Tree view
are good for seeing dependencies and the Gantt view is good for understanding durations.

## Local Development

Airflow and Composer have certain restrictions on Python dependencies that we otherwise do
not have with our other applications (like App Engine or Cloud Run). This means that we do
need a specific `pyproject.toml` within the `recidiviz/airflow` directory in order to get started with
local development.

Before developing and installing the dependencies for the first time, run

```
brew install mysql
```

To initialize a virtualenv for the Airflow package,

```
cd recidiviz/airflow
uv sync --all-extras
source .venv/bin/activate
```

If you have an M2 Apple Chip and run into a `command '/usr/bin/clang' failed with exit code 1` error when syncing that
looks similar to the issues reported [here](https://github.com/Homebrew/homebrew-core/issues/130258), try these commands
to fix:
```
brew install zlib
ln -sv $(brew --prefix zlib)/lib/libz.dylib $(brew --prefix)/lib/libzlib.dylib
```

To add new dependencies

* Edit `recidiviz/airflow/pyproject.toml` to include the new package (if needed only for tests, add to [project.optional-dependencies]).
* Push the branch.
* Trigger the "Re-lock Airflow dependencies" Github action for the branch.
* Pull changes that are added once it succeeds.
* Run the following:

```
cd recidiviz/airflow
uv sync --all-extras
```

To run unit tests,

```
cd recidiviz/airflow
uv run pytest tests
```

If you want to be able to run / debug these tests within PyCharm, you will need to set up a new Python interpreter for the `recidiviz/airflow` venv.

To exit the virtualenv and go back to the main root

```
deactivate  # exits the airflow venv
cd ../..    # go back to pulse-data root folder
source .venv/bin/activate  # activates the root venv
```

If you are adding new pulse-data code dependencies within the Airflow package, you
must update the `recidiviz/tools/validate_source_visibility.py` script to be able to
recognize those dependencies. You'll need to do this for both your test files and also
the source code files for the DAG.

The `uv.lock` will be updated every week as part of our automated dependency update
process.

## Testing DAGs in GCP

To test DAGs in GCP, you will need to create an experiment environment to upload all of your source code to.
See `recidiviz/tools/airflow/environment_control.py` for more info.

Run the following from the root virtualenv:
```
python -m recidiviz.tools.airflow.environment_control create
```

## Adding New Source Files

If you're referencing new source files from pulse-data in Airflow or you need to add
new paths within the `recidiviz/airflow` package, be sure to update the Terraform config
under `recidiviz/tools/deploy/terraform/config/cloud_composer_source_files_to_copy.yaml`.
DAG files will automatically get uploaded as part of the Terraform deploy (see `cloud-composer.tf`).
The first entry is the innermost directory that doesn't have a wildcard. The second entry
is the path that either is the file name or the wildcard pattern. See Testing for ways
to then update the experiment bucket.
