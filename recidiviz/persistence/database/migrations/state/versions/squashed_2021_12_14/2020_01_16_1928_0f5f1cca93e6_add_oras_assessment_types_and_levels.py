# pylint: skip-file
"""add_oras_assessment_types_and_levels

Revision ID: 0f5f1cca93e6
Revises: ef2616e5aaa1
Create Date: 2020-01-16 19:28:55.216974

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0f5f1cca93e6"
down_revision = "ef2616e5aaa1"
branch_labels = None
depends_on = None

# Without new values
old_type_values = ["ASI", "LSIR", "ORAS", "PSA", "SORAC"]
old_level_values = [
    "LOW",
    "LOW_MEDIUM",
    "MEDIUM",
    "MEDIUM_HIGH",
    "MODERATE",
    "HIGH",
    "NOT_APPLICABLE",
    "UNDETERMINED",
]

# With new values
new_type_values = [
    "ASI",
    "LSIR",
    "ORAS",
    "ORAS_COMMUNITY_SUPERVISION",
    "ORAS_COMMUNITY_SUPERVISION_SCREENING",
    "ORAS_MISDEMEANOR_ASSESSMENT",
    "ORAS_MISDEMEANOR_SCREENING",
    "ORAS_PRE_TRIAL",
    "ORAS_PRISON_SCREENING",
    "ORAS_PRISON_INTAKE",
    "ORAS_REENTRY",
    "ORAS_STATIC",
    "ORAS_SUPPLEMENTAL_REENTRY",
    "PSA",
    "SORAC",
]
new_level_values = [
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
    op.execute("ALTER TYPE state_assessment_type RENAME TO state_assessment_type_old;")
    sa.Enum(*new_type_values, name="state_assessment_type").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_type",
        type_=sa.Enum(*new_type_values, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_type",
        type_=sa.Enum(*new_type_values, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.execute("DROP TYPE state_assessment_type_old;")

    op.execute(
        "ALTER TYPE state_assessment_level RENAME TO state_assessment_level_old;"
    )
    sa.Enum(*new_level_values, name="state_assessment_level").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_level",
        type_=sa.Enum(*new_level_values, name="state_assessment_level"),
        postgresql_using="assessment_level::text::state_assessment_level",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_level",
        type_=sa.Enum(*new_level_values, name="state_assessment_level"),
        postgresql_using="assessment_level::text::state_assessment_level",
    )
    op.execute("DROP TYPE state_assessment_level_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_assessment_type RENAME TO state_assessment_type_old;")
    sa.Enum(*old_type_values, name="state_assessment_type").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_type",
        type_=sa.Enum(*old_type_values, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_type",
        type_=sa.Enum(*old_type_values, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.execute("DROP TYPE state_assessment_type_old;")

    op.execute(
        "ALTER TYPE state_assessment_level RENAME TO state_assessment_level_old;"
    )
    sa.Enum(*old_level_values, name="state_assessment_level").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_level",
        type_=sa.Enum(*old_level_values, name="state_assessment_level"),
        postgresql_using="assessment_level::text::state_assessment_level",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_level",
        type_=sa.Enum(*old_level_values, name="state_assessment_level"),
        postgresql_using="assessment_level::text::state_assessment_level",
    )
    op.execute("DROP TYPE state_assessment_level_old;")
