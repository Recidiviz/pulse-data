# pylint: skip-file
"""adjust_nd_charge

Revision ID: 2cad1cf3061a
Revises: c2b1cc617c51
Create Date: 2022-09-13 12:15:42.670034

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2cad1cf3061a"
down_revision = "c2b1cc617c51"
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
        WHERE state_charge.charge_id = joined.charge_id AND state_charge.state_code = 'US_ND';
        """
    )


def downgrade() -> None:
    op.execute(
        "UPDATE state_charge SET judicial_district_code = NULL, judge_full_name = NULL WHERE state_code = 'US_ND';"
    )
