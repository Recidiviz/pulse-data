# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""
Adds a row with the given status and the current timestamps to the
`direct_ingest_instance_status` table in the OPERATIONS DB in order to transition
the current status.

THIS SCRIPT SHOULD BE USED WITH CAUTION AND ONLY AFTER CONSULTING WITH DOPPLER.

Example usage:
python -m recidiviz.tools.ingest.operations.set_ingest_instance_status \
    --project_id recidiviz-staging \
    --state_code US_XX \
    --ingest_instance SECONDARY \
    --new_instance_status NO_RAW_DATA_REIMPORT_IN_PROGRESS
"""
import argparse
from datetime import datetime

import pytz

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gating import is_ingest_in_dataflow_enabled
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations.schema import (
    DirectIngestInstanceStatus,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        dest="project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--state_code",
        type=StateCode,
        choices=list(StateCode),
        help="The state code to set the ingest status for.",
    )
    parser.add_argument(
        "--ingest_instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="The ingest instance to set the ingest status for.",
    )
    parser.add_argument(
        "--new_instance_status",
        type=DirectIngestStatus,
        required=True,
        help="The new status to transition this state's instance to.",
    )

    return parser.parse_args()


def main(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    new_instance_status: DirectIngestStatus,
) -> None:
    """Updates the instance's status to |new_instance_status| after prompting the
    user.
    """
    prompt_for_confirmation(
        "!!! Setting an instance status manually should be a relatively rare "
        "operation. You should only run this script if you are VERY CERTAIN and have "
        "consulted with Doppler. Continue?"
    )

    schema_type = SchemaType.OPERATIONS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(schema_type=schema_type):
        with SessionFactory.for_proxy(database_key) as session:
            manager = DirectIngestInstanceStatusManager(
                region_code=state_code.value,
                ingest_instance=ingest_instance,
                is_ingest_in_dataflow_enabled=is_ingest_in_dataflow_enabled(
                    state_code=state_code, instance=ingest_instance
                ),
            )
            # pylint: disable=protected-access
            current_status = manager._get_current_status_row(session).status
            prompt_for_confirmation(
                f"Setting the {state_code.value} {ingest_instance.value} "
                f"status from {current_status.value} to "
                f"{new_instance_status.value} - continue?",
            )
            new_row = DirectIngestInstanceStatus(
                region_code=manager.region_code,
                instance=manager.ingest_instance.value,
                status_timestamp=datetime.now(tz=pytz.UTC),
                status=new_instance_status.value,
            )
            session.add(new_row)
            session.commit()
            current_status = manager._get_current_status_row(session).status
            print(f"Status now set to {current_status.value}")


if __name__ == "__main__":
    args = parse_arguments()
    with local_project_id_override(args.project_id):
        main(args.state_code, args.ingest_instance, args.new_instance_status)
