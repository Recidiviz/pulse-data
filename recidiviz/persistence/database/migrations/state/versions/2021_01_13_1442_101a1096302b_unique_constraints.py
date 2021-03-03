# pylint: skip-file
"""unique_constraints

Revision ID: 101a1096302b
Revises: a23546daf24b
Create Date: 2021-01-13 14:42:36.831582

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "101a1096302b"
down_revision = "a23546daf24b"
branch_labels = None
depends_on = None


def upgrade():
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY bond_external_ids_unique_within_state_index "
            "ON state_bond (state_code, external_id);"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY early_discharge_external_ids_unique_within_state_index "
            "ON state_early_discharge (state_code, external_id);"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY incarceration_period_external_ids_unique_within_state_index "
            "ON state_incarceration_period (state_code, external_id);"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY fine_external_ids_unique_within_state_index "
            "ON state_fine (state_code, external_id);"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY parole_decision_external_ids_unique_within_state_index "
            "ON state_parole_decision (state_code, external_id);"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY person_external_ids_unique_within_type_and_region_index "
            "ON state_person_external_id (state_code, external_id, id_type);"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY program_assignment_external_ids_unique_within_state_index "
            "ON state_program_assignment (state_code, external_id);"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY supervision_case_type_entry_external_ids_unique_within_state_index "
            "ON state_supervision_case_type_entry (state_code, external_id);"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY supervision_contact_external_ids_unique_within_state_index "
            "ON state_supervision_contact (state_code, external_id);"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY supervision_violation_external_ids_unique_within_state_index "
            "ON state_supervision_violation (state_code, external_id);"
        )
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY supervision_violation_response_external_ids_unique_within_state_index "
            "ON state_supervision_violation_response (state_code, external_id);"
        )
    op.execute(
        "ALTER TABLE state_bond ADD CONSTRAINT bond_external_ids_unique_within_state "
        "UNIQUE USING INDEX bond_external_ids_unique_within_state_index;"
    )
    op.execute(
        "ALTER TABLE state_early_discharge ADD CONSTRAINT early_discharge_external_ids_unique_within_state "
        "UNIQUE USING INDEX early_discharge_external_ids_unique_within_state_index;"
    )
    op.execute(
        "ALTER TABLE state_fine ADD CONSTRAINT fine_external_ids_unique_within_state "
        "UNIQUE USING INDEX fine_external_ids_unique_within_state_index;"
    )
    op.execute(
        "ALTER TABLE state_incarceration_period ADD CONSTRAINT incarceration_period_external_ids_unique_within_state "
        "UNIQUE USING INDEX incarceration_period_external_ids_unique_within_state_index;"
    )
    op.execute(
        "ALTER TABLE state_parole_decision ADD CONSTRAINT parole_decision_external_ids_unique_within_state "
        "UNIQUE USING INDEX parole_decision_external_ids_unique_within_state_index;"
    )
    op.execute(
        "ALTER TABLE state_person_external_id ADD CONSTRAINT person_external_ids_unique_within_type_and_region "
        "UNIQUE USING INDEX person_external_ids_unique_within_type_and_region_index;"
    )
    op.drop_constraint(
        "external_ids_unique_within_type_and_region",
        "state_person_external_id",
        type_="unique",
    )
    op.execute(
        "ALTER TABLE state_program_assignment ADD CONSTRAINT program_assignment_external_ids_unique_within_state "
        "UNIQUE USING INDEX program_assignment_external_ids_unique_within_state_index;"
    )
    op.execute(
        "ALTER TABLE state_supervision_case_type_entry ADD CONSTRAINT supervision_case_type_entry_external_ids_unique_within_state "
        "UNIQUE USING INDEX supervision_case_type_entry_external_ids_unique_within_state_index;"
    )
    op.execute(
        "ALTER TABLE state_supervision_contact ADD CONSTRAINT supervision_contact_external_ids_unique_within_state "
        "UNIQUE USING INDEX supervision_contact_external_ids_unique_within_state_index;"
    )
    op.execute(
        "ALTER TABLE state_supervision_violation ADD CONSTRAINT supervision_violation_external_ids_unique_within_state "
        "UNIQUE USING INDEX supervision_violation_external_ids_unique_within_state_index;"
    )
    op.execute(
        "ALTER TABLE state_supervision_violation_response ADD CONSTRAINT supervision_violation_response_external_ids_unique_within_state "
        "UNIQUE USING INDEX supervision_violation_response_external_ids_unique_within_state_index;"
    )


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(
        "supervision_violation_response_external_ids_unique_within_state",
        "state_supervision_violation_response",
        type_="unique",
    )
    op.drop_constraint(
        "supervision_violation_external_ids_unique_within_state",
        "state_supervision_violation",
        type_="unique",
    )
    op.drop_constraint(
        "supervision_contact_external_ids_unique_within_state",
        "state_supervision_contact",
        type_="unique",
    )
    op.drop_constraint(
        "supervision_case_type_entry_external_ids_unique_within_state",
        "state_supervision_case_type_entry",
        type_="unique",
    )
    op.drop_constraint(
        "program_assignment_external_ids_unique_within_state",
        "state_program_assignment",
        type_="unique",
    )
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE UNIQUE INDEX CONCURRENTLY external_ids_unique_within_type_and_region_index "
            "ON state_person_external_id (state_code, external_id);"
        )
    op.execute(
        "ALTER TABLE state_person_external_id ADD CONSTRAINT external_ids_unique_within_type_and_region "
        "UNIQUE USING INDEX external_ids_unique_within_type_and_region_index;"
    )
    # op.create_unique_constraint('external_ids_unique_within_type_and_region', 'state_person_external_id', ['state_code', 'id_type', 'external_id'])
    op.drop_constraint(
        "person_external_ids_unique_within_type_and_region",
        "state_person_external_id",
        type_="unique",
    )
    op.drop_constraint(
        "parole_decision_external_ids_unique_within_state",
        "state_parole_decision",
        type_="unique",
    )
    op.drop_constraint(
        "incarceration_period_external_ids_unique_within_state",
        "state_incarceration_period",
        type_="unique",
    )
    op.drop_constraint(
        "fine_external_ids_unique_within_state", "state_fine", type_="unique"
    )
    op.drop_constraint(
        "early_discharge_external_ids_unique_within_state",
        "state_early_discharge",
        type_="unique",
    )
    op.drop_constraint(
        "bond_external_ids_unique_within_state", "state_bond", type_="unique"
    )
    # ### end Alembic commands ###
