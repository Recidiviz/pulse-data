# pylint: skip-file
"""adjust_tn_charge

Revision ID: c2b1cc617c51
Revises: f3a221230485
Create Date: 2022-09-12 13:59:56.261212

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c2b1cc617c51"
down_revision = "f3a221230485"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        WITH joined AS (
            SELECT state_charge.charge_id, state_court_case.judicial_district_code, 
            state_agent.full_name 
            FROM state_charge LEFT JOIN state_court_case 
            ON state_charge.court_case_id = state_court_case.court_case_id
            LEFT JOIN state_agent ON state_court_case.judge_id = state_agent.agent_id
        )
        UPDATE state_charge SET judicial_district_code = joined.judicial_district_code,
        judge_full_name = joined.full_name
        FROM joined
        WHERE state_charge.charge_id = joined.charge_id AND state_charge.state_code = 'US_TN';
        """
    )


def downgrade() -> None:
    op.execute(
        "UPDATE state_charge SET judicial_district_code = NULL, judge_full_name = NULL WHERE state_code = 'US_TN';"
    )
