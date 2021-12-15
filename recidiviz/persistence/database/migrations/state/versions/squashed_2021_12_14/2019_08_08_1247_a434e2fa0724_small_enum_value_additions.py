# pylint: skip-file
"""small-enum-value-additions

Revision ID: a434e2fa0724
Revises: d9a007bfbb70
Create Date: 2019-08-08 12:47:25.535471

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a434e2fa0724"
down_revision = "d9a007bfbb70"
branch_labels = None
depends_on = None


# Without new value
old_supervision_values = ["EXTERNAL_UNKNOWN"]
old_court_case_values = ["EXTERNAL_UNKNOWN"]

# With new value
new_supervision_values = [
    "EXTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "MINIMUM",
    "MEDIUM",
    "MAXIMUM",
    "DIVERSION",
    "INTERSTATE_COMPACT",
]
new_court_case_values = ["EXTERNAL_UNKNOWN", "PRESENT_WITHOUT_INFO"]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_level RENAME TO state_supervision_level_old;"
    )
    sa.Enum(*new_supervision_values, name="state_supervision_level").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_level",
        type_=sa.Enum(*new_supervision_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_level",
        type_=sa.Enum(*new_supervision_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.execute("DROP TYPE state_supervision_level_old;")

    op.execute(
        "ALTER TYPE state_court_case_status RENAME TO state_court_case_status_old;"
    )
    sa.Enum(*new_court_case_values, name="state_court_case_status").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_court_case",
        column_name="status",
        type_=sa.Enum(*new_court_case_values, name="state_court_case_status"),
        postgresql_using="status::text::state_court_case_status",
    )
    op.alter_column(
        "state_court_case_history",
        column_name="status",
        type_=sa.Enum(*new_court_case_values, name="state_court_case_status"),
        postgresql_using="status::text::state_court_case_status",
    )
    op.execute("DROP TYPE state_court_case_status_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_level RENAME TO state_supervision_level_old;"
    )
    sa.Enum(*old_supervision_values, name="state_supervision_level").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_level",
        type_=sa.Enum(*old_supervision_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_level",
        type_=sa.Enum(*old_supervision_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.execute("DROP TYPE state_supervision_level_old;")

    op.execute(
        "ALTER TYPE state_court_case_status RENAME TO state_court_case_status_old;"
    )
    sa.Enum(*old_court_case_values, name="state_court_case_status").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_court_case",
        column_name="status",
        type_=sa.Enum(*old_court_case_values, name="state_court_case_status"),
        postgresql_using="status::text::state_court_case_status",
    )
    op.alter_column(
        "state_court_case_history",
        column_name="status",
        type_=sa.Enum(*old_court_case_values, name="state_court_case_status"),
        postgresql_using="status::text::state_court_case_status",
    )
    op.execute("DROP TYPE state_court_case_status_old;")
