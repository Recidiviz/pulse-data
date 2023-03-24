# pylint: skip-file
"""fill_contacting_staff_id_fields

Revision ID: 3a92c60dff11
Revises: 71d81c77ea70
Create Date: 2023-03-22 12:34:22.772511

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3a92c60dff11"
down_revision = "71d81c77ea70"
branch_labels = None
depends_on = None

TN_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_assessment LEFT JOIN state_agent
        ON state_assessment.conducting_agent_id = state_agent.agent_id
    )
    UPDATE state_assessment
    SET conducting_staff_external_id = joined.external_id,
    conducting_staff_external_id_type = 'US_TN_STAFF_TOMIS'
    FROM joined
    WHERE state_assessment.state_code = 'US_TN' AND joined.external_id IS NOT NULL;
"""

ME_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_assessment LEFT JOIN state_agent
        ON state_assessment.conducting_agent_id = state_agent.agent_id
    )
    UPDATE state_assessment
    SET conducting_staff_external_id = joined.external_id,
    conducting_staff_external_id_type = 'US_ME_EMPLOYEE'
    FROM joined
    WHERE state_assessment.state_code = 'US_ME' AND joined.external_id IS NOT NULL;
"""

MI_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_assessment LEFT JOIN state_agent
        ON state_assessment.conducting_agent_id = state_agent.agent_id
    )
    UPDATE state_assessment
    SET conducting_staff_external_id = joined.external_id,
    conducting_staff_external_id_type = 'US_MI_COMPAS_USER'
    FROM joined
    WHERE state_assessment.state_code = 'US_MI' AND joined.external_id IS NOT NULL;
"""

SET_TO_NULL_QUERY = """
    UPDATE state_assessment
    SET conducting_staff_external_id = NULL,
    conducting_staff_external_id_type = NULL  
"""


def upgrade() -> None:
    connection = op.get_bind()  # type: ignore [attr-defined]
    connection.execute(TN_COPY_QUERY)
    connection.execute(ME_COPY_QUERY)
    connection.execute(MI_COPY_QUERY)


def downgrade() -> None:
    connection = op.get_bind()  # type: ignore [attr-defined]
    connection.execute(SET_TO_NULL_QUERY)
