# pylint: skip-file
"""add_unknown_values_state_assessment_level

Revision ID: 5af550c0600d
Revises: 2b85029d9d8f
Create Date: 2022-04-14 18:24:26.084141

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5af550c0600d"
down_revision = "2b85029d9d8f"
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
    "MINIMUM",
    "MAXIMUM",
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
    "INTERNAL_UNKNOWN",
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
