# pylint: skip-file
"""add_caf

Revision ID: cfbd8b0f96c2
Revises: 8d9b12b33005
Create Date: 2023-04-24 09:39:07.967087

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "cfbd8b0f96c2"
down_revision = "8d9b12b33005"
branch_labels = None
depends_on = None

# Include CAF
new_values = [
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

# WIthout CAF
old_values = [
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
