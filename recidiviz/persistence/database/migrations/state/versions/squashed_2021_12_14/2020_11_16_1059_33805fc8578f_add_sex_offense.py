# pylint: skip-file
"""add_sex_offense

Revision ID: 33805fc8578f
Revises: b5e7a817dd27
Create Date: 2020-11-16 10:59:09.001992

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "33805fc8578f"
down_revision = "b5e7a817dd27"
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
    "SEX_OFFENDER",
    "VETERANS_COURT",
]

# With new value
new_values = [
    "ALCOHOL_DRUG",
    "DOMESTIC_VIOLENCE",
    "DRUG_COURT",
    "FAMILY_COURT",
    "GENERAL",
    "MENTAL_HEALTH_COURT",
    "SERIOUS_MENTAL_ILLNESS",
    "SEX_OFFENDER",
    "VETERANS_COURT",
    "SEX_OFFENSE",
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
    op.alter_column(
        "state_supervision_case_type_entry_history",
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
    op.alter_column(
        "state_supervision_case_type_entry_history",
        column_name="case_type",
        type_=sa.Enum(*old_values, name="state_supervision_case_type"),
        postgresql_using="case_type::text::state_supervision_case_type",
    )
    op.execute("DROP TYPE state_supervision_case_type_old;")
