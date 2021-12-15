# pylint: skip-file
"""add_assessment_level_external_unknown

Revision ID: a6181898067d
Revises: 841f9f4944f9
Create Date: 2020-09-29 23:48:31.381937

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a6181898067d"
down_revision = "841f9f4944f9"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "LOW",
    "LOW_MEDIUM",
    "MEDIUM",
    "MEDIUM_HIGH",
    "MODERATE",
    "HIGH",
    "VERY_HIGH",
    "NOT_APPLICABLE",
    "UNDETERMINED",
]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "LOW",
    "LOW_MEDIUM",
    "MEDIUM",
    "MEDIUM_HIGH",
    "MODERATE",
    "HIGH",
    "VERY_HIGH",
    "NOT_APPLICABLE",
    "UNDETERMINED",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_assessment_level RENAME TO state_assessment_level_old;"
    )
    sa.Enum(*new_values, name="state_assessment_level").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_level",
        type_=sa.Enum(*new_values, name="state_assessment_level"),
        postgresql_using="assessment_level::text::state_assessment_level",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_level",
        type_=sa.Enum(*new_values, name="state_assessment_level"),
        postgresql_using="assessment_level::text::state_assessment_level",
    )
    op.execute("DROP TYPE state_assessment_level_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_assessment_level RENAME TO state_assessment_level_old;"
    )
    sa.Enum(*old_values, name="state_assessment_level").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_level",
        type_=sa.Enum(*old_values, name="state_assessment_level"),
        postgresql_using="assessment_level::text::state_assessment_level",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_level",
        type_=sa.Enum(*old_values, name="state_assessment_level"),
        postgresql_using="assessment_level::text::state_assessment_level",
    )
    op.execute("DROP TYPE state_assessment_level_old;")
