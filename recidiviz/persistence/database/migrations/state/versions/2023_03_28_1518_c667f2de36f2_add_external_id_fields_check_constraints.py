# pylint: skip-file
"""add_external_id_fields_check_constraints

Revision ID: c667f2de36f2
Revises: 5d33eb64dcb1
Create Date: 2023-03-28 15:18:03.284389

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "c667f2de36f2"
down_revision = "5d33eb64dcb1"
branch_labels = None
depends_on = None

ASSESSMENT_CONSTRAINT = "conducting_staff_external_id_fields_consistent"
SUPERVISION_PERIOD_CONSTRAINT = (
    "supervising_officer_staff_external_id_fields_consistent"
)
PROGRAM_ASSIGNMENT_CONSTRAINT = "referring_staff_external_id_fields_consistent"
SUPERVISION_CONTACT_CONSTRAINT = "contacting_staff_external_id_fields_consistent"

CONDITION_TEMPLATE = """
    ({id_field} IS NULL AND {id_field}_type IS NULL)
    OR ({id_field} IS NOT NULL AND {id_field}_type IS NOT NULL)
"""


def upgrade() -> None:
    op.create_check_constraint(
        constraint_name=ASSESSMENT_CONSTRAINT,
        table_name="state_assessment",
        condition=StrictStringFormatter().format(
            CONDITION_TEMPLATE,
            id_field="conducting_staff_external_id",
        ),
    )

    op.create_check_constraint(
        constraint_name=SUPERVISION_PERIOD_CONSTRAINT,
        table_name="state_supervision_period",
        condition=StrictStringFormatter().format(
            CONDITION_TEMPLATE,
            id_field="supervising_officer_staff_external_id",
        ),
    )

    op.create_check_constraint(
        constraint_name=PROGRAM_ASSIGNMENT_CONSTRAINT,
        table_name="state_program_assignment",
        condition=StrictStringFormatter().format(
            CONDITION_TEMPLATE,
            id_field="referring_staff_external_id",
        ),
    )

    op.create_check_constraint(
        constraint_name=SUPERVISION_CONTACT_CONSTRAINT,
        table_name="state_supervision_contact",
        condition=StrictStringFormatter().format(
            CONDITION_TEMPLATE,
            id_field="contacting_staff_external_id",
        ),
    )


def downgrade() -> None:
    op.drop_constraint(
        constraint_name=ASSESSMENT_CONSTRAINT,
        table_name="state_assessment",
        type_="check",
    )
    op.drop_constraint(
        constraint_name=SUPERVISION_PERIOD_CONSTRAINT,
        table_name="state_supervision_period",
        type_="check",
    )
    op.drop_constraint(
        constraint_name=PROGRAM_ASSIGNMENT_CONSTRAINT,
        table_name="state_program_assignment",
        type_="check",
    )
    op.drop_constraint(
        constraint_name=SUPERVISION_CONTACT_CONSTRAINT,
        table_name="state_supervision_contact",
        type_="check",
    )
