# pylint: skip-file
"""add_contacting_staff_id_fields

Revision ID: 228b7f58a338
Revises: c667f2de36f2
Create Date: 2023-03-30 10:44:32.594729

"""
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "228b7f58a338"
down_revision = "c667f2de36f2"
branch_labels = None
depends_on = None


COPY_QUERY_TEMPLATE = """
    WITH joined AS (
        -- Filter join table down to as few rows as possible
        SELECT
            state_supervision_contact.supervision_contact_id,
            state_agent.external_id,
            '{external_id_type}' AS contacting_staff_external_id_type
        FROM
            state_supervision_contact
        LEFT JOIN
            state_agent
        ON state_supervision_contact.contacted_agent_id = state_agent.agent_id
        WHERE
            state_supervision_contact.state_code = '{state_code}'
            AND state_agent.external_id IS NOT NULL
    )
    UPDATE state_supervision_contact
    SET
        contacting_staff_external_id = joined.external_id,
        contacting_staff_external_id_type = joined.contacting_staff_external_id_type
    FROM joined
    WHERE
        -- Only update with info from the appropriate row in the join table
        state_supervision_contact.supervision_contact_id = joined.supervision_contact_id;
"""

SET_TO_NULL_QUERY = """
    UPDATE state_supervision_contact
    SET contacting_staff_external_id = NULL,
    contacting_staff_external_id_type = NULL  
"""


def upgrade() -> None:

    for state_code, id_type in [
        ("US_TN", "US_TN_STAFF_TOMIS"),
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
    connection = op.get_bind()  # type: ignore [attr-defined]
    connection.execute(SET_TO_NULL_QUERY)
