# pylint: skip-file
"""drop-unused-sup-case-type

Revision ID: bdad4b108bec
Revises: 0bf70c925d11
Create Date: 2022-05-06 17:56:47.301897

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bdad4b108bec"
down_revision = "0bf70c925d11"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "ALCOHOL_DRUG",
    "DOMESTIC_VIOLENCE",
    "DRUG_COURT",
    "FAMILY_COURT",
    "GENERAL",
    "MENTAL_HEALTH_COURT",
    "SERIOUS_MENTAL_ILLNESS",
    "SEX_OFFENSE",
    "VETERANS_COURT",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# With new value
new_values = [
    "DOMESTIC_VIOLENCE",
    "DRUG_COURT",
    "FAMILY_COURT",
    "GENERAL",
    "MENTAL_HEALTH_COURT",
    "SERIOUS_MENTAL_ILLNESS",
    "SEX_OFFENSE",
    "VETERANS_COURT",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_case_type RENAME TO state_supervision_case_type_old;"
    )
    sa.Enum(*new_values, name="state_supervision_case_type").create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_case_type_entry",
        column_name="case_type",
        type_=sa.Enum(*new_values, name="state_supervision_case_type"),
        postgresql_using="case_type::text::state_supervision_case_type",
    )
    op.execute("DROP TYPE state_supervision_case_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_case_type RENAME TO state_supervision_case_type_old;"
    )
    sa.Enum(*old_values, name="state_supervision_case_type").create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_case_type_entry",
        column_name="case_type",
        type_=sa.Enum(*old_values, name="state_supervision_case_type"),
        postgresql_using="case_type::text::state_supervision_case_type",
    )
    op.execute("DROP TYPE state_supervision_case_type_old;")
