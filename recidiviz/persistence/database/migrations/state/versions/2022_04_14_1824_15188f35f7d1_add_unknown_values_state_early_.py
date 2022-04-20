# pylint: skip-file
"""add_unknown_values_state_early_discharge_decision

Revision ID: 15188f35f7d1
Revises: 5af550c0600d
Create Date: 2022-04-14 18:24:29.577526

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "15188f35f7d1"
down_revision = "5af550c0600d"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "REQUEST_DENIED",
    "SENTENCE_TERMINATION_GRANTED",
    "UNSUPERVISED_PROBATION_GRANTED",
]

# With new value
new_values = [
    "REQUEST_DENIED",
    "SENTENCE_TERMINATION_GRANTED",
    "UNSUPERVISED_PROBATION_GRANTED",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_early_discharge_decision RENAME TO state_early_discharge_decision_old;"
    )
    sa.Enum(*new_values, name="state_early_discharge_decision").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_early_discharge",
        column_name="decision",
        type_=sa.Enum(*new_values, name="state_early_discharge_decision"),
        postgresql_using="decision::text::state_early_discharge_decision",
    )
    op.alter_column(
        "state_early_discharge_history",
        column_name="decision",
        type_=sa.Enum(*new_values, name="state_early_discharge_decision"),
        postgresql_using="decision::text::state_early_discharge_decision",
    )
    op.execute("DROP TYPE state_early_discharge_decision_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_early_discharge_decision RENAME TO state_early_discharge_decision_old;"
    )
    sa.Enum(*old_values, name="state_early_discharge_decision").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_early_discharge",
        column_name="decision",
        type_=sa.Enum(*old_values, name="state_early_discharge_decision"),
        postgresql_using="decision::text::state_early_discharge_decision",
    )
    op.alter_column(
        "state_early_discharge_history",
        column_name="decision",
        type_=sa.Enum(*old_values, name="state_early_discharge_decision"),
        postgresql_using="decision::text::state_early_discharge_decision",
    )
    op.execute("DROP TYPE state_early_discharge_decision_old;")
