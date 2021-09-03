# pylint: skip-file
"""delete_oras_assessment_type

Revision ID: cc9bd09e8e85
Revises: b0d11bd54316
Create Date: 2021-09-02 10:32:32.252374

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "cc9bd09e8e85"
down_revision = "b0d11bd54316"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "INTERNAL_UNKNOWN",
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

# With new value
new_values = [
    "INTERNAL_UNKNOWN",
    "ASI",
    "CSSM",
    "HIQ",
    "LSIR",
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
