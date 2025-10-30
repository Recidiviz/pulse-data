# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Helpers for server setup."""
import logging
from typing import Iterable, List

from flask import Flask

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.case_triage.pathways.pathways_database_manager import (
    PathwaysDatabaseManager,
)
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions


def database_keys_for_schema_type(
    schema_type: SchemaType,
) -> List[SQLAlchemyDatabaseKey]:
    """Returns a list of keys for **all** databases in the instance corresponding to
    this schema type.
    """
    if not schema_type.is_multi_db_schema:
        return [SQLAlchemyDatabaseKey.for_schema(schema_type)]

    match schema_type:
        case SchemaType.PATHWAYS:
            return [
                PathwaysDatabaseManager.database_key_for_state(state_code)
                for state_code in get_pathways_enabled_states_for_cloud_sql()
            ]

        case SchemaType.WORKFLOWS:
            workflows_db_manager = StateSegmentedDatabaseManager(
                get_workflows_enabled_states(), SchemaType.WORKFLOWS
            )
            return [
                workflows_db_manager.database_key_for_state(state_code)
                for state_code in get_workflows_enabled_states()
            ]

        case SchemaType.INSIGHTS:
            insights_db_manager = StateSegmentedDatabaseManager(
                get_outliers_enabled_states(), SchemaType.INSIGHTS
            )
            return [
                insights_db_manager.database_key_for_state(state_code)
                for state_code in get_outliers_enabled_states()
            ]

    raise ValueError(f"Unexpected schema_type: [{schema_type}]")


def state_codes_for_schema_type(
    schema_type: SchemaType,
) -> List[str]:
    """Returns a list of state codes enabled for the instance corresponding to
    this schema type.
    """
    if not schema_type.is_multi_db_schema:
        raise ValueError(f"schema_type is not multi DB: [{schema_type}]")

    match schema_type:
        case SchemaType.PATHWAYS:
            return get_pathways_enabled_states_for_cloud_sql()

        case SchemaType.WORKFLOWS:
            return get_workflows_enabled_states()

        case SchemaType.INSIGHTS:
            return get_outliers_enabled_states()

    raise ValueError(f"Unexpected schema_type: [{schema_type}]")


def initialize_scoped_sessions(app: Flask) -> None:
    # We can connect to the development versions of our databases database using the
    # default `init_engine` configurations, which uses secrets in `recidiviz/local`. If
    # you are missing these secrets, run this script:
    # ./recidiviz/tools/admin_panel/initialize_development_environment.sh

    try:
        # Note: this only sets up scoped sessions for the Case Triage schema. If endpoints using other
        # schemas end up deciding to use scoped sessions, setup_scoped_sessions can be modified to
        # accept+bind more schemas: https://github.com/dtheodor/flask-sqlalchemy-session/issues/4
        setup_scoped_sessions(app, SchemaType.CASE_TRIAGE)
    except BaseException as e:
        logging.error(e)
        logging.warning(
            "Could not initialize engine for %s - have you run `initialize_development_environment.sh`?",
            SchemaType.CASE_TRIAGE,
        )


def initialize_engines(schema_types: Iterable[SchemaType]) -> None:
    # This attempts to connect a list of our databases. Any connections that fail will
    # be logged and not raise an error, so that a single database outage doesn't take
    # down the entire application. Any attempt to use those databases later will
    # attempt to connect again in case the database was just unhealthy.
    for schema_type in schema_types:
        # TODO(#23253): Remove when Publisher is migrated to JC GCP project.
        secret_prefix_override = (
            JUSTICE_COUNTS_DB_SECRET_PREFIX
            if schema_type == SchemaType.JUSTICE_COUNTS
            else None
        )
        try:
            SQLAlchemyEngineManager.init_engine(
                database_key=SQLAlchemyDatabaseKey.for_schema(schema_type),
                secret_prefix_override=secret_prefix_override,
            )
        except BaseException as e:
            logging.error(e)
            logging.warning(
                "Could not initialize engine for %s - have you run `initialize_development_environment.sh`?",
                schema_type,
            )
