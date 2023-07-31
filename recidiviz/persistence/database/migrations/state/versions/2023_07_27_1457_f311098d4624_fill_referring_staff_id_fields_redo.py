# pylint: skip-file
"""fill_referring_staff_id_fields_redo

Revision ID: f311098d4624
Revises: d0868b07c36e
Create Date: 2023-07-27 14:57:17.918093

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "f311098d4624"
down_revision = "d0868b07c36e"
branch_labels = None
depends_on = None

COPY_QUERY_TEMPLATE = """
    WITH joined AS (
        -- Filter join table down to as few rows as possible
        SELECT
            state_program_assignment.program_assignment_id,
            state_agent.external_id,
            '{external_id_type}' AS referring_staff_external_id_type
        FROM
            state_program_assignment
        LEFT JOIN
            state_agent
        ON state_program_assignment.referring_agent_id = state_agent.agent_id
        WHERE
            state_program_assignment.state_code = '{state_code}'
            AND state_agent.external_id IS NOT NULL
    )
    UPDATE state_program_assignment
    SET
        referring_staff_external_id = joined.external_id,
        referring_staff_external_id_type = joined.referring_staff_external_id_type
    FROM joined
    WHERE
        -- Only update with info from the appropriate row in the join table
        state_program_assignment.program_assignment_id = joined.program_assignment_id;
"""

SET_TO_NULL_QUERY = """
    UPDATE state_program_assignment
    SET referring_staff_external_id = NULL,
    referring_staff_external_id_type = NULL  
"""


def upgrade() -> None:

    for state_code, id_type in [
        ("US_TN", "US_TN_STAFF_TOMIS"),
        ("US_ME", "US_ME_EMPLOYEE"),
        ("US_MI", "US_MI_COMPAS_USER"),
        ("US_MO", "US_MO_STAFF_BADGE_NUMBER"),
        ("US_PA", "US_PA_PBPP_EMPLOYEE_NUM"),
        ("US_IX", "US_IX_EMPLOYEE"),
        ("US_ND", "US_ND_DOCSTARS_OFFICER"),
    ]:
        op.execute(
            StrictStringFormatter().format(
                COPY_QUERY_TEMPLATE,
                state_code=state_code,
                external_id_type=id_type,
            )
        )


def downgrade() -> None:
    op.execute(SET_TO_NULL_QUERY)
