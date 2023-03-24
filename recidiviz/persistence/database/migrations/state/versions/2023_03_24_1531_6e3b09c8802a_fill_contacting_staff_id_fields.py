# pylint: skip-file
"""fill_contacting_staff_id_fields

Revision ID: 6e3b09c8802a
Revises: a12cf982c3a0
Create Date: 2023-03-24 15:31:53.435676

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6e3b09c8802a"
down_revision = "a12cf982c3a0"
branch_labels = None
depends_on = None

ND_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_contact LEFT JOIN state_agent
        ON state_supervision_contact.contacted_agent_id = state_agent.agent_id
    )
    UPDATE state_supervision_contact
    SET contacting_staff_external_id = joined.external_id,
    contacting_staff_external_id_type = 'US_ND_DOCSTARS_OFFICER'
    FROM joined
    WHERE state_supervision_contact.state_code = 'US_ND' AND joined.external_id IS NOT NULL;
"""

TN_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_contact LEFT JOIN state_agent
        ON state_supervision_contact.contacted_agent_id = state_agent.agent_id
    )
    UPDATE state_supervision_contact
    SET contacting_staff_external_id = joined.external_id,
    contacting_staff_external_id_type = 'US_TN_STAFF_TOMIS'
    FROM joined
    WHERE state_supervision_contact.state_code = 'US_TN' AND joined.external_id IS NOT NULL;
"""

PA_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_contact LEFT JOIN state_agent
        ON state_supervision_contact.contacted_agent_id = state_agent.agent_id
    )
    UPDATE state_supervision_contact
    SET contacting_staff_external_id = joined.external_id,
    contacting_staff_external_id_type = 'US_PA_PBPP_EMPLOYEE_NUM'
    FROM joined
    WHERE state_supervision_contact.state_code = 'US_PA' AND joined.external_id IS NOT NULL;
"""

IX_COPY_QUERY = """
    WITH joined AS (
        SELECT state_agent.external_id
        FROM state_supervision_contact LEFT JOIN state_agent
        ON state_supervision_contact.contacted_agent_id = state_agent.agent_id
    )
    UPDATE state_supervision_contact
    SET contacting_staff_external_id = joined.external_id,
    contacting_staff_external_id_type = 'US_IX_EMPLOYEE'
    FROM joined
    WHERE state_supervision_contact.state_code = 'US_IX' AND joined.external_id IS NOT NULL;
"""


SET_TO_NULL_QUERY = """
    UPDATE state_supervision_contact
    SET contacting_staff_external_id = NULL,
    contacting_staff_external_id_type = NULL  
"""


def upgrade() -> None:
    connection = op.get_bind()  # type: ignore [attr-defined]
    connection.execute(ND_COPY_QUERY)
    connection.execute(TN_COPY_QUERY)
    connection.execute(PA_COPY_QUERY)
    connection.execute(IX_COPY_QUERY)


def downgrade() -> None:
    connection = op.get_bind()  # type: ignore [attr-defined]
    connection.execute(SET_TO_NULL_QUERY)
