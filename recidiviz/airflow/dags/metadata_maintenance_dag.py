"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid
having too much data in your Airflow MetaStore.

## Authors

The DAG is a fork of [teamclairvoyant repository.](
https://github.com/teamclairvoyant/airflow-maintenance-dags/tree/master/db-cleanup
)

## Usage

1. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME,
  ALERT_EMAIL_ADDRESSES and ENABLE_DELETE) in the DAG with the desired values

2. Modify the DATABASE_OBJECTS list to add/remove objects as needed. Each
   dictionary in the list features the following parameters:
    - airflow_db_model: Model imported from airflow.models corresponding to
      a table in the airflow metadata database
    - age_check_column: Column in the model/table to use for calculating max
      date of data deletion

3. Create and Set the following Variables in the Airflow Web Server
  (Admin -> Variables)
    - airflow_db_cleanup__max_db_entry_age_in_days - integer - Length to
      retain the log files if not already provided in the conf. If this is set
      to 30, the job will remove those files that are 30 days old or older.

4. Put the DAG in your gcs bucket.
"""
# pylint: disable=ungrouped-imports disable=wrong-import-order
import logging
from datetime import datetime, timedelta
from typing import Any, TypedDict

import airflow
import dateutil.parser
from airflow import settings
from airflow.models import (
    DAG,
    Base,
    DagModel,
    DagRun,
    Log,
    SlaMiss,
    TaskInstance,
    Variable,
    XCom,
)
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.version import version as airflow_version
from sqlalchemy import Column, desc, sql, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Query, Session

from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_metadata_maintenance_dag_id,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.utils.types import assert_type

now = timezone.utcnow

# airflow-db-cleanup
DAG_ID = get_metadata_maintenance_dag_id(get_project_id())
START_DATE = airflow.utils.dates.days_ago(1)
# How often to Run. @daily - Once a day at Midnight (UTC)
SCHEDULE_INTERVAL = "@daily"
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "operations"
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = ["alerts@recidiviz.org"]
# Airflow version used by the environment in list form, value stored in
# airflow_version is in format e.g "2.3.4+composer"
AIRFLOW_VERSION = airflow_version[: -len("+composer")].split(".")
# Length to retain the log files if not already provided in the conf. If this
# is set to 120, the job will remove those files that are 120 days old or older.
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = int(
    Variable.get("airflow_db_cleanup__max_db_entry_age_in_days", 120)
)
# Prints the database entries which will be getting deleted; set to False
# to avoid printing large lists and slowdown process
PRINT_DELETES = False
# Whether the job should delete the db entries or not. Included if you want to
# temporarily avoid deleting the db entries.
ENABLE_DELETE = True


class ParamsType(TypedDict):
    airflow_db_model: Base
    age_check_column: Column
    keep_last: bool
    keep_last_filters: list | None
    keep_last_group_by: Column | None
    do_not_delete_by_dag_id: bool | None


# List of all the objects that will be deleted. Comment out the DB objects you
# want to skip.
DATABASE_OBJECTS = [
    {
        "airflow_db_model": DagRun,
        "age_check_column": DagRun.execution_date,
        "keep_last": True,
        "keep_last_filters": [DagRun.external_trigger.is_(False)],
        "keep_last_group_by": DagRun.dag_id,
    },
    {
        "airflow_db_model": TaskInstance,
        "age_check_column": TaskInstance.start_date
        if AIRFLOW_VERSION < ["2", "2", "0"]
        else TaskInstance.start_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": Log,
        "age_check_column": Log.dttm,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": XCom,
        "age_check_column": XCom.execution_date
        if AIRFLOW_VERSION < ["2", "2", "5"]
        else XCom.timestamp,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": SlaMiss,
        "age_check_column": SlaMiss.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
    {
        "airflow_db_model": DagModel,
        "age_check_column": DagModel.last_parsed_time,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None,
    },
]

# Check for TaskReschedule model
try:
    from airflow.models import TaskReschedule

    DATABASE_OBJECTS.append(
        {
            "airflow_db_model": TaskReschedule,
            "age_check_column": TaskReschedule.execution_date
            if AIRFLOW_VERSION < ["2", "2", "0"]
            else TaskReschedule.start_date,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None,
        }
    )

except Exception as e:
    logging.error(e)

# Check for TaskFail model
try:
    from airflow.models import TaskFail

    DATABASE_OBJECTS.append(
        {
            "airflow_db_model": TaskFail,
            "age_check_column": TaskFail.start_date,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None,
        }
    )

except Exception as e:
    logging.error(e)

# Check for RenderedTaskInstanceFields model
if AIRFLOW_VERSION < ["2", "4", "0"]:
    try:
        from airflow.models import RenderedTaskInstanceFields

        DATABASE_OBJECTS.append(
            {
                "airflow_db_model": RenderedTaskInstanceFields,
                "age_check_column": RenderedTaskInstanceFields.execution_date,
                "keep_last": False,
                "keep_last_filters": None,
                "keep_last_group_by": None,
            }
        )

    except Exception as e:
        logging.error(e)

# Check for ImportError model
try:
    from airflow.models import ImportError as AirflowImportError

    DATABASE_OBJECTS.append(
        {
            "airflow_db_model": AirflowImportError,
            "age_check_column": AirflowImportError.timestamp,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None,
            "do_not_delete_by_dag_id": True,
        }
    )

except Exception as e:
    logging.error(e)

try:
    from airflow.jobs.job import Job

    DATABASE_OBJECTS.append(
        {
            "airflow_db_model": Job,
            "age_check_column": Job.latest_heartbeat,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None,
        }
    )
except Exception as e:
    logging.error(e)

default_args = {
    "owner": DAG_OWNER_NAME,
    "depends_on_past": False,
    "email": ALERT_EMAIL_ADDRESSES,
    "email_on_failure": True,
    "email_on_retry": False,
    "start_date": START_DATE,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    render_template_as_native_obj=True,
)
if hasattr(dag, "doc_md"):
    dag.doc_md = __doc__
if hasattr(dag, "catchup"):
    dag.catchup = False


def print_configuration_function(**context: dict[Any, Any]) -> None:
    logging.info("Loading Configurations...")
    dag_run = assert_type(context.get("dag_run"), DagRun)
    dag_run_conf = dag_run.conf
    logging.info("dag_run.conf: %s", str(dag_run_conf))
    max_db_entry_age_in_days = None
    if dag_run_conf:
        max_db_entry_age_in_days = dag_run_conf.get("maxDBEntryAgeInDays", None)
    logging.info("maxDBEntryAgeInDays from dag_run.conf: %s", str(dag_run_conf))
    if max_db_entry_age_in_days is None or max_db_entry_age_in_days < 1:
        logging.info(
            "maxDBEntryAgeInDays conf variable isn't included or Variable value is less than 1. Using Default '%s'",
            str(DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS),
        )
        max_db_entry_age_in_days = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS
    max_date = now() + timedelta(-assert_type(max_db_entry_age_in_days, int))
    logging.info("Finished Loading Configurations")
    logging.info("")

    logging.info("Configurations:")
    logging.info("max_db_entry_age_in_days: %s", str(max_db_entry_age_in_days))
    logging.info("max_date:                 %s", str(max_date))
    logging.info("enable_delete:            %s", str(ENABLE_DELETE))
    logging.info("")

    logging.info("Setting max_execution_date to XCom for Downstream Processes")
    task_instance = assert_type(context["ti"], TaskInstance)
    task_instance.xcom_push(key="max_date", value=max_date.isoformat())


print_configuration = PythonOperator(
    task_id="print_configuration",
    python_callable=print_configuration_function,
    provide_context=True,
    dag=dag,
)


def build_query(
    session: Session,
    airflow_db_model: Base,
    age_check_column: Column,
    max_date: datetime,
    dag_id: str | None = None,
) -> Query:
    """
    Build a database query to retrieve and filter Airflow data.

    Args:
        session: SQLAlchemy session object for database interaction.
        airflow_db_model: The Airflow model class to query (e.g., DagRun).
        age_check_column: The column representing the age of the data.
        max_date: The maximum allowed age for the data.
        dag_id (optional): The ID of the DAG to filter by. Defaults to None.

    Returns:
        SQLAlchemy query object: The constructed query.
    """
    query = session.query(airflow_db_model)

    logging.info("INITIAL QUERY : %s", str(query))

    if dag_id:
        query = query.filter(airflow_db_model.dag_id == dag_id)

    if airflow_db_model == DagRun:
        # For DaRus we want to leave last DagRun regardless of its age
        newest_dagrun = (
            session.query(airflow_db_model)
            .filter(airflow_db_model.dag_id == dag_id)
            .order_by(desc(airflow_db_model.execution_date))
            .first()
        )
        logging.info("Newest dagrun: %s", str(newest_dagrun))
        if newest_dagrun is not None:
            query = (
                query.filter(DagRun.external_trigger.is_(False))
                .filter(age_check_column <= max_date)
                .filter(airflow_db_model.id != newest_dagrun.id)
            )
        else:
            query = query.filter(sql.false())
    else:
        query = query.filter(age_check_column <= max_date)

    logging.info("FINAL QUERY: %s", str(query))

    return query


def print_query(query: Query, airflow_db_model: Base, age_check_column: Column) -> None:
    entries_to_delete = query.all()

    logging.info("Query: %s", str(query))
    logging.info(
        "Process will be Deleting the following %s (s):", str(airflow_db_model.__name__)
    )
    for entry in entries_to_delete:
        date = str(entry.__dict__[str(age_check_column).split(".")[1]])
        logging.info("\tEntry: %s, Date: %s", str(entry), str(date))

    logging.info(
        "Process will be Deleting %s %s (s)",
        str(len(entries_to_delete)),
        str(airflow_db_model.__name__),
    )


def cleanup_function(**context: dict[Any, Any]) -> None:
    """Performs the deletion"""
    session = settings.Session()

    logging.info("Retrieving max_execution_date from XCom")
    task_instance = assert_type(context["ti"], TaskInstance)
    max_date = task_instance.xcom_pull(
        task_ids=print_configuration.task_id, key="max_date"
    )
    max_date = dateutil.parser.parse(max_date)  # stored as iso8601 str in xcom

    params = assert_type(context["params"], ParamsType)
    airflow_db_model = assert_type(params.get("airflow_db_model"), Base)
    state = params.get("state")
    age_check_column = params.get("age_check_column")
    keep_last = params.get("keep_last")
    keep_last_filters = params.get("keep_last_filters")
    keep_last_group_by = params.get("keep_last_group_by")

    logging.info("Configurations:")
    logging.info("max_date:                 %s", str(max_date))
    logging.info("enable_delete:            %s", str(ENABLE_DELETE))
    logging.info("session:                  %s", str(session))
    logging.info("airflow_db_model:         %s", str(airflow_db_model))
    logging.info("state:                    %s", str(state))
    logging.info("age_check_column:         %s", str(age_check_column))
    logging.info("keep_last:                %s", str(keep_last))
    logging.info("keep_last_filters:        %s", str(keep_last_filters))
    logging.info("keep_last_group_by:       %s", str(keep_last_group_by))

    logging.info("")

    logging.info("Running Cleanup Process...")

    try:
        if params.get("do_not_delete_by_dag_id"):
            query = build_query(
                session=session,
                airflow_db_model=airflow_db_model,
                age_check_column=age_check_column,
                max_date=max_date,
            )
            if PRINT_DELETES:
                print_query(query, airflow_db_model, age_check_column)
            if ENABLE_DELETE:
                logging.info("Performing Delete...")
                query.delete(synchronize_session=False)
            session.commit()
        else:
            if not hasattr(airflow_db_model, "dag_id"):
                raise ValueError(
                    "Cannot delete model by dag_id if it does not have dag_id"
                )

            dags = session.query(airflow_db_model.dag_id).distinct()
            session.commit()

            list_dags = [str(list(dag)[0]) for dag in dags] + [None]
            for dag_id in list_dags:
                query = build_query(
                    session=session,
                    airflow_db_model=airflow_db_model,
                    age_check_column=age_check_column,
                    max_date=max_date,
                    dag_id=dag_id,
                )
                if PRINT_DELETES:
                    print_query(query, airflow_db_model, age_check_column)
                if ENABLE_DELETE:
                    logging.info("Performing Delete...")
                    query.delete(synchronize_session=False)
                session.commit()

        if not ENABLE_DELETE:
            logging.warning(
                "You've opted to skip deleting the db entries. "
                "Set ENABLE_DELETE to True to delete entries!!!"
            )

        logging.info("Finished Running Cleanup Process")

    except ProgrammingError as programming_error:
        logging.error(programming_error)
        logging.error(
            "%s is not present in the metadata. Skipping...", str(airflow_db_model)
        )

    finally:
        session.close()


def cleanup_sessions() -> None:
    session = settings.Session()

    try:
        logging.info("Deleting sessions...")
        count_statement = (
            "SELECT COUNT(*) AS cnt FROM session "
            + "WHERE expiry < now()::timestamp(0);"
        )
        before = session.execute(text(count_statement)).one_or_none()["cnt"]
        session.execute(text("DELETE FROM session WHERE expiry < now()::timestamp(0);"))
        after = session.execute(text(count_statement)).one_or_none()["cnt"]
        logging.info("Deleted %s expired sessions.", (before - after))
    except Exception as err:
        logging.exception(err)

    session.commit()
    session.close()


def analyze_db() -> None:
    session = settings.Session()
    session.execute("ANALYZE")
    session.commit()
    session.close()


analyze_op = PythonOperator(
    task_id="analyze_query", python_callable=analyze_db, provide_context=True, dag=dag
)

cleanup_session_op = PythonOperator(
    task_id="cleanup_sessions",
    python_callable=cleanup_sessions,
    provide_context=True,
    dag=dag,
)

cleanup_session_op.set_downstream(analyze_op)

for db_object in DATABASE_OBJECTS:
    cleanup_op = PythonOperator(
        task_id="cleanup_" + str(db_object["airflow_db_model"].__name__),
        python_callable=cleanup_function,
        params=db_object,
        provide_context=True,
        dag=dag,
    )

    print_configuration.set_downstream(cleanup_op)
    cleanup_op.set_downstream(analyze_op)
