# pylint: skip-file
"""add_case_type_supervision_levels

Revision ID: cd6e39d46c5f
Revises: 4f84ccf378b8
Create Date: 2020-06-25 19:42:03.281094

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "cd6e39d46c5f"
down_revision = "4f84ccf378b8"
branch_labels = None
depends_on = None

# Without new value
old_case_type_values = [
    "DOMESTIC_VIOLENCE",
    "GENERAL",
    "SERIOUS_MENTAL_ILLNESS",
    "SEX_OFFENDER",
]
old_supervision_level_values = [
    "INTERNAL_UNKNOWN",
    "MEDIUM",
    "HIGH",
    "INCARCERATED",
    "MAXIMUM",
    "INTERSTATE_COMPACT",
    "PRESENT_WITHOUT_INFO",
    "MINIMUM",
    "DIVERSION",
    "EXTERNAL_UNKNOWN",
]

# With new value
new_case_type_values = [
    "DOMESTIC_VIOLENCE",
    "DRUG_COURT",
    "FAMILY_COURT",
    "GENERAL",
    "MENTAL_HEALTH_COURT",
    "SERIOUS_MENTAL_ILLNESS",
    "SEX_OFFENDER",
    "VETERANS_COURT",
]
new_supervision_level_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "DIVERSION",
    "INCARCERATED",
    "INTERSTATE_COMPACT",
    "IN_CUSTODY",
    "LIMITED",
    "MINIMUM",
    "MEDIUM",
    "HIGH",
    "MAXIMUM",
    "UNSUPERVISED",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_case_type RENAME TO state_supervision_case_type_old;"
    )
    sa.Enum(*new_case_type_values, name="state_supervision_case_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_case_type_entry",
        column_name="case_type",
        type_=sa.Enum(*new_case_type_values, name="state_supervision_case_type"),
        postgresql_using="case_type::text::state_supervision_case_type",
    )
    op.alter_column(
        "state_supervision_case_type_entry_history",
        column_name="case_type",
        type_=sa.Enum(*new_case_type_values, name="state_supervision_case_type"),
        postgresql_using="case_type::text::state_supervision_case_type",
    )
    op.execute("DROP TYPE state_supervision_case_type_old;")

    op.execute(
        "ALTER TYPE state_supervision_level RENAME TO state_supervision_level_old;"
    )
    sa.Enum(*new_supervision_level_values, name="state_supervision_level").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_level",
        type_=sa.Enum(*new_supervision_level_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_level",
        type_=sa.Enum(*new_supervision_level_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.execute("DROP TYPE state_supervision_level_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_case_type RENAME TO state_supervision_case_type_old;"
    )
    sa.Enum(*old_case_type_values, name="state_supervision_case_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_case_type_entry",
        column_name="case_type",
        type_=sa.Enum(*old_case_type_values, name="state_supervision_case_type"),
        postgresql_using="case_type::text::state_supervision_case_type",
    )
    op.alter_column(
        "state_supervision_case_type_entry_history",
        column_name="case_type",
        type_=sa.Enum(*old_case_type_values, name="state_supervision_case_type"),
        postgresql_using="case_type::text::state_supervision_case_type",
    )
    op.execute("DROP TYPE state_supervision_case_type_old;")

    op.execute(
        "ALTER TYPE state_supervision_level RENAME TO state_supervision_level_old;"
    )
    sa.Enum(*old_supervision_level_values, name="state_supervision_level").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_level",
        type_=sa.Enum(*old_supervision_level_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_level",
        type_=sa.Enum(*old_supervision_level_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.execute("DROP TYPE state_supervision_level_old;")
