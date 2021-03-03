"""add_id_decisions_to_violation_response_decisions

Revision ID: cc51102fcaba
Revises: fe0a2a58f82f
Create Date: 2020-04-10 17:22:05.637190

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "cc51102fcaba"
down_revision = "fe0a2a58f82f"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "CONTINUANCE",
    "DELAYED_ACTION",
    "EXTENSION",
    "PRIVILEGES_REVOKED",
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
    "SHOCK_INCARCERATION",
    "SPECIALIZED_COURT",
    "SUSPENSION",
    "TREATMENT_IN_PRISON",
]


def upgrade():
    op.execute(
        "ALTER TYPE state_supervision_violation_response_decision RENAME TO state_supervision_violation_response_decision_old;"
    )
    sa.Enum(*new_values, name="state_supervision_violation_response_decision").create(
        bind=op.get_bind()
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
    op.execute("DROP TYPE state_supervision_violation_response_decision_old;")


def downgrade():
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
