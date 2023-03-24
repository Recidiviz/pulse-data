# pylint: skip-file
"""fill_supervising_officer_staff_id_fields

Revision ID: a12cf982c3a0
Revises: 3a92c60dff11
Create Date: 2023-03-24 12:28:07.728329

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a12cf982c3a0"
down_revision = "3a92c60dff11"
branch_labels = None
depends_on = None

MO_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_period LEFT JOIN state_agent
        ON state_supervision_period.supervising_officer_id = state_agent.agent_id
    )
    UPDATE state_supervision_period
    SET supervising_officer_staff_external_id = joined.external_id,
    supervising_officer_staff_external_id_type = 'US_MO_STAFF_BADGE_NUMBER'
    FROM joined
    WHERE state_supervision_period.state_code = 'US_MO' AND joined.external_id IS NOT NULL;
"""

MI_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_period LEFT JOIN state_agent
        ON state_supervision_period.supervising_officer_id = state_agent.agent_id
    )
    UPDATE state_supervision_period
    SET supervising_officer_staff_external_id = joined.external_id,
    supervising_officer_staff_external_id_type = 'US_MI_COMPAS_USER'
    FROM joined
    WHERE state_supervision_period.state_code = 'US_MI' AND joined.external_id IS NOT NULL;
"""

ME_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_period LEFT JOIN state_agent
        ON state_supervision_period.supervising_officer_id = state_agent.agent_id
    )
    UPDATE state_supervision_period
    SET supervising_officer_staff_external_id = joined.external_id,
    supervising_officer_staff_external_id_type = 'US_ME_EMPLOYEE'
    FROM joined
    WHERE state_supervision_period.state_code = 'US_ME' AND joined.external_id IS NOT NULL;
"""

PA_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_period LEFT JOIN state_agent
        ON state_supervision_period.supervising_officer_id = state_agent.agent_id
    )
    UPDATE state_supervision_period
    SET supervising_officer_staff_external_id = joined.external_id,
    supervising_officer_staff_external_id_type = 'US_PA_PBPP_EMPLOYEE_NUM'
    FROM joined
    WHERE state_supervision_period.state_code = 'US_PA' AND joined.external_id IS NOT NULL;
"""

TN_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_period LEFT JOIN state_agent
        ON state_supervision_period.supervising_officer_id = state_agent.agent_id
    )
    UPDATE state_supervision_period
    SET supervising_officer_staff_external_id = joined.external_id,
    supervising_officer_staff_external_id_type = 'US_TN_STAFF_TOMIS'
    FROM joined
    WHERE state_supervision_period.state_code = 'US_TN' AND joined.external_id IS NOT NULL;
"""

IX_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_period LEFT JOIN state_agent
        ON state_supervision_period.supervising_officer_id = state_agent.agent_id
    )
    UPDATE state_supervision_period
    SET supervising_officer_staff_external_id = joined.external_id,
    supervising_officer_staff_external_id_type = 'US_IX_EMPLOYEE'
    FROM joined
    WHERE state_supervision_period.state_code = 'US_IX' AND joined.external_id IS NOT NULL;
"""

ND_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_period LEFT JOIN state_agent
        ON state_supervision_period.supervising_officer_id = state_agent.agent_id
    )
    UPDATE state_supervision_period
    SET supervising_officer_staff_external_id = joined.external_id,
    supervising_officer_staff_external_id_type = 'US_ND_DOCSTARS_OFFICER'
    FROM joined
    WHERE state_supervision_period.state_code = 'US_ND' AND joined.external_id IS NOT NULL;
"""

SET_TO_NULL_QUERY = """
    UPDATE state_supervision_period
    SET supervising_officer_staff_external_id = NULL,
    supervising_officer_staff_external_id_type = NULL  
"""


def upgrade() -> None:
    connection = op.get_bind()  # type: ignore [attr-defined]
    connection.execute(MO_COPY_QUERY)
    connection.execute(MI_COPY_QUERY)
    connection.execute(ME_COPY_QUERY)
    connection.execute(PA_COPY_QUERY)
    connection.execute(TN_COPY_QUERY)
    connection.execute(IX_COPY_QUERY)
    connection.execute(ND_COPY_QUERY)


def downgrade() -> None:
    connection = op.get_bind()  # type: ignore [attr-defined]
    connection.execute(SET_TO_NULL_QUERY)
