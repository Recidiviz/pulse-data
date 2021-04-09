# pylint: skip-file
"""update-revocation-type

Revision ID: e5949964b987
Revises: 42c3e60d0887
Create Date: 2020-07-02 15:10:55.452109

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e5949964b987"
down_revision = "42c3e60d0887"
branch_labels = None
depends_on = None


WARRANT_QUERY = (
    "SELECT supervision_violation_response_decision_entry_id FROM"
    " state_supervision_violation_response_decision_entry"
    " WHERE state_code = 'US_MO' AND decision_raw_text = 'A'"
)

SHOCK_INCARCERATION_QUERY = (
    "SELECT supervision_violation_response_decision_entry_id FROM"
    " state_supervision_violation_response_decision_entry"
    " WHERE state_code = 'US_MO' AND decision_raw_text = 'CO'"
)

UPDATE_TO_NEW_VALUE_QUERY = (
    "UPDATE state_supervision_violation_response_decision_entry"
    " SET revocation_type = '{new_value}', revocation_type_raw_text = '{new_raw_text_value}'"
    " WHERE supervision_violation_response_decision_entry_id IN ({ids_query});"
)

SET_TO_NULL_QUERY = (
    "UPDATE state_supervision_violation_response_decision_entry SET revocation_type = NULL,"
    " revocation_type_raw_text = NULL"
    " WHERE supervision_violation_response_decision_entry_id IN ({ids_query});"
)


def upgrade() -> None:
    connection = op.get_bind()

    # Set the revocation_type and revocation_type_raw_text to NULL for all 'A' (WARRANT_ISSUED) decisions
    connection.execute(SET_TO_NULL_QUERY.format(ids_query=WARRANT_QUERY))

    updated_revocation_type_shock = "SHOCK_INCARCERATION"

    # Set the revocation_type and revocation_type_raw_text to SHOCK_INCARCERATION for all 'CO'
    # (SHOCK_INCARCERATION) decisions
    connection.execute(
        UPDATE_TO_NEW_VALUE_QUERY.format(
            new_value=updated_revocation_type_shock,
            new_raw_text_value=updated_revocation_type_shock,
            ids_query=SHOCK_INCARCERATION_QUERY,
        )
    )


def downgrade() -> None:
    connection = op.get_bind()

    treatment_in_prison_value = "TREATMENT_IN_PRISON"
    reincarceration_value = "REINCARCERATION"

    # Set the revocation_type and revocation_type_raw_text back to 'REINCARCERATION' for all 'A' (WARRANT_ISSUED)
    # decisions
    connection.execute(
        UPDATE_TO_NEW_VALUE_QUERY.format(
            new_value=reincarceration_value,
            new_raw_text_value=reincarceration_value,
            ids_query=WARRANT_QUERY,
        )
    )

    # Set the revocation_type and revocation_type_raw_text back to TREATMENT_IN_PRISON for all 'CO'
    # (SHOCK_INCARCERATION) decisions
    connection.execute(
        UPDATE_TO_NEW_VALUE_QUERY.format(
            new_value=treatment_in_prison_value,
            new_raw_text_value=treatment_in_prison_value,
            ids_query=SHOCK_INCARCERATION_QUERY,
        )
    )
