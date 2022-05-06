# pylint: skip-file
"""drop-unused-assessment-classes

Revision ID: 09a00a11ebcb
Revises: f53996d14a5d
Create Date: 2022-05-06 16:03:47.004175

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "09a00a11ebcb"
down_revision = "f53996d14a5d"
branch_labels = None
depends_on = None

# With MENTAL_HEALTH and SECURITY_CLASSIFICATION
old_values = [
    "MENTAL_HEALTH",
    "RISK",
    "SECURITY_CLASSIFICATION",
    "SEX_OFFENSE",
    "SOCIAL",
    "SUBSTANCE_ABUSE",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# Without MENTAL_HEALTH and SECURITY_CLASSIFICATION
new_values = [
    "RISK",
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
