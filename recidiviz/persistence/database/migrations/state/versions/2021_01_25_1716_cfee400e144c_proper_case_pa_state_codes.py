# pylint: skip-file
"""proper_case_pa_state_codes

Revision ID: cfee400e144c
Revises: 101a1096302b
Create Date: 2021-01-25 17:16:00.460982

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "cfee400e144c"
down_revision = "101a1096302b"
branch_labels = None
depends_on = None


UPDATE_TO_NEW_VALUE_QUERY = (
    "UPDATE {table} SET state_code = 'US_PA' WHERE state_code = 'us_pa';"
)

TABLE_NAMES = [
    "state_agent",
    "state_assessment",
    "state_bond",
    "state_charge",
    "state_court_case",
    "state_early_discharge",
    "state_fine",
    "state_incarceration_incident",
    "state_incarceration_incident_outcome",
    "state_incarceration_period",
    "state_incarceration_sentence",
    "state_parole_decision",
    "state_person",
    "state_person_alias",
    "state_person_ethnicity",
    "state_person_external_id",
    "state_person_race",
    "state_program_assignment",
    "state_sentence_group",
    "state_supervision_case_type_entry",
    "state_supervision_contact",
    "state_supervision_period",
    "state_supervision_sentence",
    "state_supervision_violated_condition_entry",
    "state_supervision_violation",
    "state_supervision_violation_response",
    "state_supervision_violation_response_decision_entry",
    "state_supervision_violation_type_entry",
]


def upgrade():
    with op.get_context().autocommit_block():
        for table_name in TABLE_NAMES:
            op.execute(UPDATE_TO_NEW_VALUE_QUERY.format(table=table_name))
            op.execute(UPDATE_TO_NEW_VALUE_QUERY.format(table=f"{table_name}_history"))


def downgrade():
    # This migration is lossy - no way to downgrade
    pass
