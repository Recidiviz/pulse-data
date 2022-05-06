# pylint: skip-file
"""drop-unused-assessment-types

Revision ID: f53996d14a5d
Revises: dbf7688df462
Create Date: 2022-05-06 16:01:21.889634

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f53996d14a5d"
down_revision = "312e157fbc9f"
branch_labels = None
depends_on = None

# With ASI and ORAS_STATIC
old_values = [
    "ASI",
    "CSSM",
    "HIQ",
    "LSIR",
    "PA_RST",
    "PSA",
    "SORAC",
    "STATIC_99",
    "TCU_DRUG_SCREEN",
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
    "INTERNAL_UNKNOWN",
    "STRONG_R",
    "J_SOAP",
    "ODARA",
    "OYAS",
    "SOTIPS",
    "SPIN_W",
    "STABLE",
    "EXTERNAL_UNKNOWN",
]

# Without ASI and ORAS_STATIC
new_values = [
    "CSSM",
    "HIQ",
    "LSIR",
    "PA_RST",
    "PSA",
    "SORAC",
    "STATIC_99",
    "TCU_DRUG_SCREEN",
    "ORAS_COMMUNITY_SUPERVISION",
    "ORAS_COMMUNITY_SUPERVISION_SCREENING",
    "ORAS_MISDEMEANOR_ASSESSMENT",
    "ORAS_MISDEMEANOR_SCREENING",
    "ORAS_PRE_TRIAL",
    "ORAS_PRISON_SCREENING",
    "ORAS_PRISON_INTAKE",
    "ORAS_REENTRY",
    "ORAS_SUPPLEMENTAL_REENTRY",
    "INTERNAL_UNKNOWN",
    "STRONG_R",
    "J_SOAP",
    "ODARA",
    "OYAS",
    "SOTIPS",
    "SPIN_W",
    "STABLE",
    "EXTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute("ALTER TYPE state_assessment_type RENAME TO state_assessment_type_old;")
    sa.Enum(*new_values, name="state_assessment_type").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_type",
        type_=sa.Enum(*new_values, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_type",
        type_=sa.Enum(*new_values, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.execute("DROP TYPE state_assessment_type_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_assessment_type RENAME TO state_assessment_type_old;")
    sa.Enum(*old_values, name="state_assessment_type").create(bind=op.get_bind())
    op.alter_column(
        "state_assessment",
        column_name="assessment_type",
        type_=sa.Enum(*old_values, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.alter_column(
        "state_assessment_history",
        column_name="assessment_type",
        type_=sa.Enum(*old_values, name="state_assessment_type"),
        postgresql_using="assessment_type::text::state_assessment_type",
    )
    op.execute("DROP TYPE state_assessment_type_old;")
