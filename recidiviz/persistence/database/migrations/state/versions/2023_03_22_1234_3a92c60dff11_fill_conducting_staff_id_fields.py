# pylint: skip-file
"""fill_conducting_staff_id_fields

Revision ID: 3a92c60dff11
Revises: 71d81c77ea70
Create Date: 2023-03-22 12:34:22.772511

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "3a92c60dff11"
down_revision = "71d81c77ea70"
branch_labels = None
depends_on = None

COPY_QUERY_TEMPLATE = """
    WITH joined AS (
        -- Filter join table down to as few rows as possible
        SELECT 
            state_assessment.assessment_id,
            state_agent.external_id,
            '{external_id_type}' AS conducting_staff_external_id_type
        FROM 
            state_assessment 
        LEFT JOIN 
            state_agent
        ON state_assessment.conducting_agent_id = state_agent.agent_id
        WHERE
            state_assessment.state_code = '{state_code}' 
            AND state_agent.external_id IS NOT NULL
    )
    UPDATE state_assessment
    SET 
        conducting_staff_external_id = joined.external_id,
        conducting_staff_external_id_type = joined.conducting_staff_external_id_type
    FROM joined
    WHERE 
        -- Only update with info from the appropriate row in the join table
        state_assessment.assessment_id = joined.assessment_id;
"""

SET_TO_NULL_QUERY = """
    UPDATE state_assessment
    SET conducting_staff_external_id = NULL,
    conducting_staff_external_id_type = NULL  
"""


def upgrade() -> None:
    for state_code, id_type in [
        ("US_TN", "US_TN_STAFF_TOMIS"),
        ("US_ME", "US_ME_EMPLOYEE"),
        ("US_MI", "US_MI_COMPAS_USER"),
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
