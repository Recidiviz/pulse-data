# pylint: skip-file
"""privileges_revoked_decision

Revision ID: 10f86afc929c
Revises: fc12db0546e3
Create Date: 2019-12-13 15:38:45.907611

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "10f86afc929c"
down_revision = "fc12db0546e3"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "CONTINUANCE",
    "DELAYED_ACTION",
    "EXTENSION",
    "REVOCATION",
    "SERVICE_TERMINATION",
    "SUSPENSION",
]

# With new value
new_values = [
    "CONTINUANCE",
    "DELAYED_ACTION",
    "EXTENSION",
    "PRIVILEGES_REVOKED",
    "REVOCATION",
    "SERVICE_TERMINATION",
    "SUSPENSION",
]


def upgrade() -> None:
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
        "state_supervision_violation_response_decision_entry",
        column_name="decision",
        type_=sa.Enum(
            *new_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )
    op.alter_column(
        "state_supervision_violation_response_decision_entry_history",
        column_name="decision",
        type_=sa.Enum(
            *new_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )

    op.execute("DROP TYPE state_supervision_violation_response_decision_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_violation_response_decision RENAME TO state_supervision_violation_response_decision_old;"
    )
    sa.Enum(*old_values, name="state_supervision_violation_response_decision").create(
        bind=op.get_bind()
    )

    op.alter_column(
        "state_supervision_violation_response_decision_entry",
        column_name="decision",
        type_=sa.Enum(
            *old_values, name="state_supervision_violation_response_decision"
        ),
        postgresql_using="decision::text::state_supervision_violation_response_decision",
    )
    op.alter_column(
        "state_supervision_violation_response_decision_entry_history",
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
