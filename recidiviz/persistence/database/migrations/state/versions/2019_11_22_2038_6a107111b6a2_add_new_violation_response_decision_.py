"""add_new_violation_response_decision_types

Revision ID: 6a107111b6a2
Revises: 481077bd8e34
Create Date: 2019-11-22 20:38:41.652852

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6a107111b6a2"
down_revision = "481077bd8e34"
branch_labels = None
depends_on = None

# Without new value
old_values = ["CONTINUANCE", "EXTENSION", "REVOCATION", "SUSPENSION"]

# With new value
new_values = [
    "CONTINUANCE",
    "DELAYED_ACTION",
    "EXTENSION",
    "REVOCATION",
    "SERVICE_TERMINATION",
    "SUSPENSION",
]


def upgrade():
    op.execute(
        "ALTER TYPE state_supervision_violation_response_decision RENAME TO state_supervision_violation_response_decision_old;"
    )
    sa.Enum(*new_values, name="state_supervision_violation_response_decision").create(
        bind=op.get_bind()
    )

    op.alter_column(
        "state_supervision_violation_response",
        column_name="decision",
        type_=sa.Enum(
            *new_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )
    op.alter_column(
        "state_supervision_violation_response_history",
        column_name="decision",
        type_=sa.Enum(
            *new_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )

    op.alter_column(
        "state_supervision_violation_response_decision_type_entry",
        column_name="decision",
        type_=sa.Enum(
            *new_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )
    op.alter_column(
        "state_supervision_violation_response_decision_type_entry_history",
        column_name="decision",
        type_=sa.Enum(
            *new_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )

    op.execute("DROP TYPE state_supervision_violation_response_decision_old;")


def downgrade():
    op.execute(
        "ALTER TYPE state_supervision_violation_response_decision RENAME TO state_supervision_violation_response_decision_old;"
    )
    sa.Enum(*old_values, name="state_supervision_violation_response_decision").create(
        bind=op.get_bind()
    )

    op.alter_column(
        "state_supervision_violation_response_decision_type_entry",
        column_name="decision",
        type_=sa.Enum(
            *old_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )
    op.alter_column(
        "state_supervision_violation_response_decision_type_entry_history",
        column_name="decision",
        type_=sa.Enum(
            *old_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )

    op.alter_column(
        "state_supervision_violation_response",
        column_name="decision",
        type_=sa.Enum(
            *old_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )
    op.alter_column(
        "state_supervision_violation_response_history",
        column_name="decision",
        type_=sa.Enum(
            *old_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )

    op.execute("DROP TYPE state_supervision_violation_response_decision_old;")
