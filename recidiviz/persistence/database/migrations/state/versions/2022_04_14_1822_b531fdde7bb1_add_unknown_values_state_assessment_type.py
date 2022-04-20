# pylint: skip-file
"""add_unknown_values_state_assessment_type

Revision ID: b531fdde7bb1
Revises: 95bdef50a8cd
Create Date: 2022-04-14 18:22:10.587900

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b531fdde7bb1"
down_revision = "95bdef50a8cd"
branch_labels = None
depends_on = None

# Without new value
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
]

# With new value
new_values = [
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
