# pylint: skip-file
"""add_initial_ingest_statuses

Revision ID: e9ebdb3b43b3
Revises: 7f4b006b4530
Create Date: 2022-08-31 14:20:18.686357

"""
import datetime
from typing import List

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "e9ebdb3b43b3"
down_revision = "7f4b006b4530"
branch_labels = None
depends_on = None

required_states = [
    "US_TN",
    "US_MO",
    "US_ME",
    "US_ND",
    "US_ID",
    "US_CO",
    "US_PA",
    "US_CA",
    "US_MI",
    "US_OZ",
    "US_IX",
]

instance_to_initial_status = {
    "PRIMARY": "STANDARD_RERUN_STARTED",
    "SECONDARY": "NO_RERUN_IN_PROGRESS",
}

status_row = """('{state_code}', '{timestamp}', '{instance}', '{status}')"""

UPGRADE_QUERY = """INSERT INTO direct_ingest_instance_status (region_code, timestamp, instance, status) VALUES
{all_status_rows};
"""


def upgrade() -> None:
    timestamp = datetime.datetime.now().isoformat()
    all_status_rows: List[str] = []
    for state_code in required_states:
        for instance, status in instance_to_initial_status.items():
            new_status_row = StrictStringFormatter().format(
                status_row,
                state_code=state_code,
                timestamp=timestamp,
                instance=instance,
                status=status,
            )
            all_status_rows.append(new_status_row)

    op.execute(
        StrictStringFormatter().format(
            UPGRADE_QUERY, all_status_rows=",\n".join(all_status_rows)
        )
    )


def downgrade() -> None:
    formatted_state_list = ", ".join(
        f"'{state_code}'" for state_code in required_states
    )
    op.execute(
        f"""
            DELETE FROM direct_ingest_instance_status
            WHERE region_code in ({formatted_state_list});
        """
    )
