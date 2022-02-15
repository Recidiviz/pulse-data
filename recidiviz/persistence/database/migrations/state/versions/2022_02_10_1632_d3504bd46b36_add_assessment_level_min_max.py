# pylint: skip-file
"""add_assessment_level_min_max

Revision ID: d3504bd46b36
Revises: 70cc56ccaf75
Create Date: 2022-02-10 16:32:40.985136

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d3504bd46b36"
down_revision = "70cc56ccaf75"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "EXTERNAL_UNKNOWN",
    "LOW",
    "LOW_MEDIUM",
    "MEDIUM",
    "MEDIUM_HIGH",
    "MODERATE",
    "HIGH",
    "VERY_HIGH",
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
    "MINIMUM",
    "MAXIMUM",
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
