# pylint: skip-file
"""add_assessment_type_strong_r

Revision ID: dfad4970cfdd
Revises: fdc33c726da0
Create Date: 2022-02-02 20:46:13.163780

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "dfad4970cfdd"
down_revision = "fdc33c726da0"
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
