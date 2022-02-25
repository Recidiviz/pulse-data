# pylint: skip-file
"""add_new_violation_response_decisions

Revision ID: 517becd19d29
Revises: d3504bd46b36
Create Date: 2022-02-16 15:20:34.455072

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "517becd19d29"
down_revision = "d3504bd46b36"
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
