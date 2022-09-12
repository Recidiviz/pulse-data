# pylint: skip-file
"""adjust_me_charge

Revision ID: 48e5766efe5c
Revises: 244b56c165ac
Create Date: 2022-09-09 13:49:37.054272

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "48e5766efe5c"
down_revision = "244b56c165ac"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE state_charge SET judicial_district_code = county_code WHERE state_code = 'US_ME';"
    )
    op.execute(
        """
        WITH joined AS (
            SELECT state_charge.charge_id, state_agent.full_name, state_agent.external_id
            FROM state_charge LEFT JOIN state_court_case 
            ON state_charge.court_case_id = state_court_case.court_case_id
            LEFT JOIN state_agent ON state_court_case.judge_id = state_agent.agent_id
        )
        UPDATE state_charge SET judge_external_id = joined.external_id,
        judge_full_name = joined.full_name
        FROM joined
        WHERE state_charge.charge_id = joined.charge_id AND state_charge.state_code = 'US_ME';
        """
    )


def downgrade() -> None:
    op.execute(
        "UPDATE state_charge SET judicial_district_code = NULL WHERE state_code = 'US_ME';"
    )
    op.execute(
        "UPDATE state_charge SET judge_external_id = NULL, judge_full_name = NULL WHERE state_code = 'US_ME';"
    )
