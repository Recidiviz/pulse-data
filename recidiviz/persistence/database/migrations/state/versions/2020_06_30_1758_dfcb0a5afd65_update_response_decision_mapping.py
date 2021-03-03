# pylint: skip-file
"""update_response_decision_mapping

Revision ID: dfcb0a5afd65
Revises: 57a4819a429c
Create Date: 2020-06-30 17:58:15.998472

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "dfcb0a5afd65"
down_revision = "57a4819a429c"
branch_labels = None
depends_on = None


WARRANT_DECISIONS_QUERY = (
    "SELECT supervision_violation_response_decision_entry_id FROM"
    " state_supervision_violation_response_decision_entry"
    " WHERE state_code = 'US_MO' AND decision_raw_text = 'A'"
)

SHOCK_INCARCERATION_QUERY = (
    "SELECT supervision_violation_response_decision_entry_id FROM"
    " state_supervision_violation_response_decision_entry"
    " WHERE state_code = 'US_MO' AND decision_raw_text = 'CO'"
)

UPDATE_QUERY = (
    "UPDATE state_supervision_violation_response_decision_entry SET decision = '{new_value}'"
    " WHERE supervision_violation_response_decision_entry_id IN ({ids_query});"
)


def upgrade():
    connection = op.get_bind()

    updated_decision_warrant = "WARRANT_ISSUED"

    connection.execute(
        UPDATE_QUERY.format(
            new_value=updated_decision_warrant, ids_query=WARRANT_DECISIONS_QUERY
        )
    )

    updated_decision_shock = "SHOCK_INCARCERATION"

    connection.execute(
        UPDATE_QUERY.format(
            new_value=updated_decision_shock, ids_query=SHOCK_INCARCERATION_QUERY
        )
    )


def downgrade():
    connection = op.get_bind()

    deprecated_decision_value = "REVOCATION"

    connection.execute(
        UPDATE_QUERY.format(
            new_value=deprecated_decision_value, ids_query=WARRANT_DECISIONS_QUERY
        )
    )

    connection.execute(
        UPDATE_QUERY.format(
            new_value=deprecated_decision_value, ids_query=SHOCK_INCARCERATION_QUERY
        )
    )
