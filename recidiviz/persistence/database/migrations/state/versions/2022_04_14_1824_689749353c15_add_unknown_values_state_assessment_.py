# pylint: skip-file
"""add_unknown_values_state_assessment_class

Revision ID: 689749353c15
Revises: 1313879b5f34
Create Date: 2022-04-14 18:24:15.581089

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "689749353c15"
down_revision = "1313879b5f34"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "MENTAL_HEALTH",
    "RISK",
    "SECURITY_CLASSIFICATION",
    "SEX_OFFENSE",
    "SOCIAL",
    "SUBSTANCE_ABUSE",
]

# With new value
new_values = [
    "MENTAL_HEALTH",
    "RISK",
    "SECURITY_CLASSIFICATION",
    "SEX_OFFENSE",
    "SOCIAL",
    "SUBSTANCE_ABUSE",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_assessment_class RENAME TO state_assessment_class_old;"
    )
    sa.Enum(*new_values, name="state_assessment_class").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_class",
        type_=sa.Enum(*new_values, name="state_assessment_class"),
        postgresql_using="assessment_class::text::state_assessment_class",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_class",
        type_=sa.Enum(*new_values, name="state_assessment_class"),
        postgresql_using="assessment_class::text::state_assessment_class",
    )
    op.execute("DROP TYPE state_assessment_class_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_assessment_class RENAME TO state_assessment_class_old;"
    )
    sa.Enum(*old_values, name="state_assessment_class").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_class",
        type_=sa.Enum(*old_values, name="state_assessment_class"),
        postgresql_using="assessment_class::text::state_assessment_class",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_class",
        type_=sa.Enum(*old_values, name="state_assessment_class"),
        postgresql_using="assessment_class::text::state_assessment_class",
    )
    op.execute("DROP TYPE state_assessment_class_old;")
