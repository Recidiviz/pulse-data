# pylint: skip-file
"""fill_supervising_officer_staff_id_fields

Revision ID: 5d33eb64dcb1
Revises: 3a92c60dff11
Create Date: 2023-03-27 12:13:11.146969

"""
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "5d33eb64dcb1"
down_revision = "3a92c60dff11"
branch_labels = None
depends_on = None

COPY_QUERY_TEMPLATE = """
    WITH joined AS (
        -- Filter join table down to as few rows as possible
        SELECT
            state_supervision_period.supervision_period_id,
            state_agent.external_id,
            '{external_id_type}' AS supervising_officer_staff_external_id_type
        FROM
            state_supervision_period
        LEFT JOIN
            state_agent
        ON state_supervision_period.supervising_officer_id = state_agent.agent_id
        WHERE
            state_supervision_period.state_code = '{state_code}'
            AND state_agent.external_id IS NOT NULL
    )
    UPDATE state_supervision_period
    SET
        supervising_officer_staff_external_id = joined.external_id,
        supervising_officer_staff_external_id_type = joined.supervising_officer_staff_external_id_type
    FROM joined
    WHERE
        -- Only update with info from the appropriate row in the join table
        state_supervision_period.supervision_period_id = joined.supervision_period_id;
"""

SET_TO_NULL_QUERY = """
    UPDATE state_supervision_period
    SET supervising_officer_staff_external_id = NULL,
    supervising_officer_staff_external_id_type = NULL  
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
