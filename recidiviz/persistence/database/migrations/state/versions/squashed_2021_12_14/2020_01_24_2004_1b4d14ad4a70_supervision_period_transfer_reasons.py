# pylint: skip-file
"""supervision_period_transfer_reasons

Revision ID: 1b4d14ad4a70
Revises: f46dbfb393e8
Create Date: 2020-01-24 20:04:50.353867

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1b4d14ad4a70"
down_revision = "f46dbfb393e8"
branch_labels = None
depends_on = None

# Without new value
old_admission_reason_values = [
    "ABSCONSION",
    "CONDITIONAL_RELEASE",
    "COURT_SENTENCE",
    "EXTERNAL_UNKNOWN",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
]  # list of all values excluding new value

old_termination_reason_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONSION",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "RETURN_FROM_ABSCONSION",
    "REVOCATION",
    "SUSPENSION",
]  # list of all values excluding new value

# With new value
new_admission_reason_values = [
    "ABSCONSION",
    "CONDITIONAL_RELEASE",
    "COURT_SENTENCE",
    "EXTERNAL_UNKNOWN",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TRANSFER_WITHIN_STATE",
]  # list of all values including new value
new_termination_reason_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONSION",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "RETURN_FROM_ABSCONSION",
    "REVOCATION",
    "SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TRANSFER_WITHIN_STATE",
]  # list of all values including new value


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_admission_reason RENAME TO state_supervision_period_admission_reason_old;"
    )
    sa.Enum(
        *new_admission_reason_values, name="state_supervision_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_admission_reason_values,
            name="state_supervision_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_admission_reason_values,
            name="state_supervision_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.execute("DROP TYPE state_supervision_period_admission_reason_old;")

    op.execute(
        "ALTER TYPE state_supervision_period_termination_reason RENAME TO state_supervision_period_termination_reason_old;"
    )
    sa.Enum(
        *new_termination_reason_values,
        name="state_supervision_period_termination_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="termination_reason",
        type_=sa.Enum(
            *new_termination_reason_values,
            name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="termination_reason",
        type_=sa.Enum(
            *new_termination_reason_values,
            name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_admission_reason RENAME TO state_supervision_period_admission_reason_old;"
    )
    sa.Enum(
        *old_admission_reason_values, name="state_supervision_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_admission_reason_values,
            name="state_supervision_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_admission_reason_values,
            name="state_supervision_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.execute("DROP TYPE state_supervision_period_admission_reason_old;")

    op.execute(
        "ALTER TYPE state_supervision_period_termination_reason RENAME TO state_supervision_period_termination_reason_old;"
    )
    sa.Enum(
        *old_termination_reason_values,
        name="state_supervision_period_termination_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="termination_reason",
        type_=sa.Enum(
            *old_termination_reason_values,
            name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="termination_reason",
        type_=sa.Enum(
            *old_termination_reason_values,
            name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")
