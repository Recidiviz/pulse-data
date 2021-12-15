# pylint: skip-file
"""add_new_assessment_classes_and_types

Revision ID: 7fcbb12b8f34
Revises: 356377cb082c
Create Date: 2020-05-27 15:49:05.601255

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7fcbb12b8f34"
down_revision = "356377cb082c"
branch_labels = None
depends_on = None

# Without new value
old_values_classes = [
    "MENTAL_HEALTH",
    "RISK",
    "SECURITY_CLASSIFICATION",
    "SUBSTANCE_ABUSE",
]
old_values_types = [
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

# With new value
new_values_classes = [
    "MENTAL_HEALTH",
    "RISK",
    "SECURITY_CLASSIFICATION",
    "SEX_OFFENSE",
    "SOCIAL",
    "SUBSTANCE_ABUSE",
]
new_values_types = [
    "ASI",
    "CSSM",
    "HIQ",
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
    "PA_RST",
    "PSA",
    "SORAC",
    "STATIC_99",
    "TCU_DRUG_SCREEN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_assessment_class RENAME TO state_assessment_class_old;"
    )
    sa.Enum(*new_values_classes, name="state_assessment_class").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_assessment",
        column_name="assessment_class",
        type_=sa.Enum(*new_values_classes, name="state_assessment_class"),
        postgresql_using="assessment_class::text::state_assessment_class",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_class",
        type_=sa.Enum(*new_values_classes, name="state_assessment_class"),
        postgresql_using="assessment_class::text::state_assessment_class",
    )
    op.execute("DROP TYPE state_assessment_class_old;")

    op.execute("ALTER TYPE state_assessment_type RENAME TO state_assessment_type_old;")
    sa.Enum(*new_values_types, name="state_assessment_type").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_type",
        type_=sa.Enum(*new_values_types, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_type",
        type_=sa.Enum(*new_values_types, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.execute("DROP TYPE state_assessment_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_assessment_class RENAME TO state_assessment_class_old;"
    )
    sa.Enum(*old_values_classes, name="state_assessment_class").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_assessment",
        column_name="assessment_class",
        type_=sa.Enum(*old_values_classes, name="state_assessment_class"),
        postgresql_using="assessment_class::text::state_assessment_class",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_class",
        type_=sa.Enum(*old_values_classes, name="state_assessment_class"),
        postgresql_using="assessment_class::text::state_assessment_class",
    )
    op.execute("DROP TYPE state_assessment_class_old;")

    op.execute("ALTER TYPE state_assessment_type RENAME TO state_assessment_type_old;")
    sa.Enum(*old_values_types, name="state_assessment_type").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_type",
        type_=sa.Enum(*old_values_types, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_type",
        type_=sa.Enum(*old_values_types, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.execute("DROP TYPE state_assessment_type_old;")
