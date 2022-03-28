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
    * http://go/airflow-experiment

Each Cloud Composer environment has an associated Google Cloud Storage bucket that holds
the source code for all of the DAGs deployed to that environment. For our orchestration
environments, we've also defined the configuration in Terraform (see `cloud-composer.tf`).

The Airflow UI provides a way to generally trigger DAGs and to see how tasks are dependent
on each other and also how long they generally take. Both the Graph view and the Tree view
are good for seeing dependencies and the Gantt view is good for understanding durations.

## Local Development

Airflow and Composer have certain restrictions on Python dependencies that we otherwise do
not have with our other applications (like App Engine or Cloud Run). This means that we do
need a specific Pipfile within the `recidiviz/airflow` directory in order to get started with
local development.

To initialize a virtualenv with this Pipfile,

```
cd recidiviz/airflow
pipenv shell
```

To add new dependencies to the Pipfile

* Edit `recidiviz/airflow/Pipfile` to include the new package (if needed only for tests, add to [dev-packages]).
* Push the branch.
* Trigger the "Re-lock Pipenv" Github action for the branch.
* Pull changes that are added once it succeeds.
* Run the following:

```
cd recidiviz/airflow
pipenv shell
pipenv sync --dev
```

To run unit tests,

```
cd recidiviz/airflow
pipenv shell
pytest tests
```

If you want to be able to run / debug these tests within PyCharm, you will need to set up a new Python interpreter for the `recidiviz/airflow` pipenv.

To exit the virtualenv and go back to the main root Pipfile

```
cd recidiviz/airflow
pipenv shell
exit # exits the airflow pipenv shell
cd ../.. # go back to pulse-data root folder
pipenv shell # starts the pipenv in pulse-data/Pipfile
```

If you are adding new pulse-data code dependencies within the Airflow package, you
must update the `recidiviz/tools/validate_source_visibility.py` script to be able to
recognize those dependencies. You'll need to do this for both your test files and also
the source code files for the DAG.

The `Pipfile.lock` will be updated every week as part of our automated dependency update
process.

## Testing DAGs in GCP

To test DAGs in GCP, you will need to first upload all of your source code to the GCS
bucket for the `experiment` Cloud Composer environment (you can see the GCS bucket by
clicking on DAGS folder). You can then refresh the appropriate DAG you're working on
and trigger a test run.
