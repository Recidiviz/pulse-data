# pylint: skip-file
"""add_dv_column

Revision ID: e1fd9273a93c
Revises: 3666e283ef4b
Create Date: 2023-10-04 09:46:04.727035

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e1fd9273a93c"
down_revision = "3666e283ef4b"
branch_labels = None
depends_on = None


# Without new value
old_values = [
    "SEX_OFFENSE",
    "ADMINISTRATIVE_SUPERVISION",
    "ALCOHOL_AND_DRUG",
    "INTENSIVE",
    "MENTAL_HEALTH",
    "ELECTRONIC_MONITORING",
    "OTHER_COURT",
    "DRUG_COURT",
    "VETERANS_COURT",
    "COMMUNITY_FACILITY",
    "OTHER",
    "GENERAL",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]
# With new value
new_values = [
    "SEX_OFFENSE",
    "ADMINISTRATIVE_SUPERVISION",
    "ALCOHOL_AND_DRUG",
    "INTENSIVE",
    "MENTAL_HEALTH",
    "ELECTRONIC_MONITORING",
    "OTHER_COURT",
    "DRUG_COURT",
    "VETERANS_COURT",
    "COMMUNITY_FACILITY",
    "DOMESTIC_VIOLENCE",
    "OTHER",
    "GENERAL",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_staff_caseload_type RENAME TO state_staff_caseload_type_old;"
    )
    sa.Enum(*new_values, name="state_staff_caseload_type").create(bind=op.get_bind())
    op.alter_column(
        "state_staff_caseload_type_period",
        column_name="caseload_type",
        type_=sa.Enum(*new_values, name="state_staff_caseload_type"),
        postgresql_using="caseload_type::text::state_staff_caseload_type",
    )
    op.execute("DROP TYPE state_staff_caseload_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_staff_caseload_type RENAME TO state_staff_caseload_type_old;"
    )
    sa.Enum(*old_values, name="state_staff_caseload_type").create(bind=op.get_bind())
    op.alter_column(
        "state_staff_caseload_type_period",
        column_name="caseload_type",
        type_=sa.Enum(*old_values, name="state_staff_caseload_type"),
        postgresql_using="caseload_type::text::state_staff_caseload_type",
    )
    op.execute("DROP TYPE state_staff_caseload_type_old;")
