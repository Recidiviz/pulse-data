# pylint: skip-file
"""pa_violation_response_decisions

Revision ID: 052c8c024b8a
Revises: 438944c4ebde
Create Date: 2020-08-14 13:07:16.715483

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "052c8c024b8a"
down_revision = "438944c4ebde"
branch_labels = None
depends_on = None


# Without new value
old_values = [
    "CONTINUANCE",
    "DELAYED_ACTION",
    "EXTENSION",
    "REVOCATION",
    "PRIVILEGES_REVOKED",
    "SERVICE_TERMINATION",
    "SPECIALIZED_COURT",
    "SHOCK_INCARCERATION",
    "SUSPENSION",
    "TREATMENT_IN_PRISON",
    "WARRANT_ISSUED",
]

# With new value
new_values = [
    "COMMUNITY_SERVICE",
    "CONTINUANCE",
    "DELAYED_ACTION",
    "EXTENSION",
    "INTERNAL_UNKNOWN",
    "NEW_CONDITIONS",
    "OTHER",
    "REVOCATION",
    "PRIVILEGES_REVOKED",
    "SERVICE_TERMINATION",
    "SPECIALIZED_COURT",
    "SHOCK_INCARCERATION",
    "SUSPENSION",
    "TREATMENT_IN_PRISON",
    "TREATMENT_IN_FIELD",
    "WARNING",
    "WARRANT_ISSUED",
]


def upgrade() -> None:
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
