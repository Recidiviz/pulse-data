# pylint: skip-file
"""drop_no_sanction_decision

Revision ID: 1142b91484ed
Revises: c0faf8fe539f
Create Date: 2022-04-20 15:17:25.335290

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1142b91484ed"
down_revision = "c0faf8fe539f"
branch_labels = None
depends_on = None

# With NO_SANCTION
old_values = [
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
    "SHOCK_INCARCERATION",
    "SPECIALIZED_COURT",
    "SUSPENSION",
    "TREATMENT_IN_PRISON",
    "TREATMENT_IN_FIELD",
    "WARNING",
    "WARRANT_ISSUED",
    "NO_SANCTION",
    "VIOLATION_UNFOUNDED",
    "EXTERNAL_UNKNOWN",
]

# Without NO_SANCTION
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
    "SHOCK_INCARCERATION",
    "SPECIALIZED_COURT",
    "SUSPENSION",
    "TREATMENT_IN_PRISON",
    "TREATMENT_IN_FIELD",
    "WARNING",
    "WARRANT_ISSUED",
    "VIOLATION_UNFOUNDED",
    "EXTERNAL_UNKNOWN",
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
    op.execute("DROP TYPE state_supervision_violation_response_decision_old;")
