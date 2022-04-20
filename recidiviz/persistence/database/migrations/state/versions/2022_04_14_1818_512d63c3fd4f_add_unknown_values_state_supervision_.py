# pylint: skip-file
"""add_unknown_values_state_supervision_violation_response_decision

Revision ID: 512d63c3fd4f
Revises: 48b454cd391d
Create Date: 2022-04-14 18:18:47.563388

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "512d63c3fd4f"
down_revision = "48b454cd391d"
branch_labels = None
depends_on = None

# Without new value
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
