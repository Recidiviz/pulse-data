# pylint: skip-file
"""add_csra

Revision ID: 1d9471924ec6
Revises: 06ac42365c32
Create Date: 2023-05-25 14:23:15.219459

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1d9471924ec6"
down_revision = "06ac42365c32"
branch_labels = None
depends_on = None


# Include CSRA
new_values = [
    "CAF",
    "CSRA",
    "CSSM",
    "COMPAS",
    "HIQ",
    "J_SOAP",
    "LSIR",
    "ODARA",
    "ORAS_COMMUNITY_SUPERVISION",
    "ORAS_COMMUNITY_SUPERVISION_SCREENING",
    "ORAS_MISDEMEANOR_ASSESSMENT",
    "ORAS_MISDEMEANOR_SCREENING",
    "ORAS_PRE_TRIAL",
    "ORAS_PRISON_SCREENING",
    "ORAS_PRISON_INTAKE",
    "ORAS_REENTRY",
    "ORAS_SUPPLEMENTAL_REENTRY",
    "OYAS",
    "PA_RST",
    "PSA",
    "SORAC",
    "SOTIPS",
    "SPIN_W",
    "STABLE",
    "STATIC_99",
    "STRONG_R",
    "TCU_DRUG_SCREEN",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# Without CSRA
old_values = [
    "CAF",
    "CSSM",
    "COMPAS",
    "HIQ",
    "J_SOAP",
    "LSIR",
    "ODARA",
    "ORAS_COMMUNITY_SUPERVISION",
    "ORAS_COMMUNITY_SUPERVISION_SCREENING",
    "ORAS_MISDEMEANOR_ASSESSMENT",
    "ORAS_MISDEMEANOR_SCREENING",
    "ORAS_PRE_TRIAL",
    "ORAS_PRISON_SCREENING",
    "ORAS_PRISON_INTAKE",
    "ORAS_REENTRY",
    "ORAS_SUPPLEMENTAL_REENTRY",
    "OYAS",
    "PA_RST",
    "PSA",
    "SORAC",
    "SOTIPS",
    "SPIN_W",
    "STABLE",
    "STATIC_99",
    "STRONG_R",
    "TCU_DRUG_SCREEN",
    "INTERNAL_UNKNOWN",
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
    op.execute("DROP TYPE state_assessment_type_old;")
