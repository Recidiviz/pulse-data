"""ip_transfer_out_of_state

Revision ID: 0c92bf530421
Revises: d0aedd5b308d
Create Date: 2020-02-26 18:25:21.176286

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0c92bf530421"
down_revision = "d0aedd5b308d"
branch_labels = None
depends_on = None

# Without new value
old_incarceration_period_admission_reason_values = [
    "ADMITTED_IN_ERROR",
    "DUAL_REVOCATION",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
]
old_incarceration_period_release_reason_values = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "EXECUTION",
    "ESCAPE",
    "EXTERNAL_UNKNOWN",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "SENTENCE_SERVED",
    "TRANSFER",
]

# With new value
new_incarceration_period_admission_reason_values = [
    "ADMITTED_IN_ERROR",
    "DUAL_REVOCATION",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
    "TRANSFERRED_FROM_OUT_OF_STATE",
]
new_incarceration_period_release_reason_values = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "EXECUTION",
    "ESCAPE",
    "EXTERNAL_UNKNOWN",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "SENTENCE_SERVED",
    "TRANSFER",
    "TRANSFERRED_OUT_OF_STATE",
]


def upgrade():
    # Update incarceration period admission reason for state incarceration period
    op.execute(
        "ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;"
    )
    sa.Enum(
        *new_incarceration_period_admission_reason_values,
        name="state_incarceration_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_incarceration_period_admission_reason_values,
            name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_incarceration_period_admission_reason_values,
            name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_admission_reason_old;")

    # Update incarceration period release reason for state incarceration period (release_reason and projected_release_reason)
    op.execute(
        "ALTER TYPE state_incarceration_period_release_reason RENAME TO state_incarceration_period_release_reason_old;"
    )
    sa.Enum(
        *new_incarceration_period_release_reason_values,
        name="state_incarceration_period_release_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="release_reason",
        type_=sa.Enum(
            *new_incarceration_period_release_reason_values,
            name="state_incarceration_period_release_reason"
        ),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="projected_release_reason",
        type_=sa.Enum(
            *new_incarceration_period_release_reason_values,
            name="state_incarceration_period_release_reason"
        ),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="release_reason",
        type_=sa.Enum(
            *new_incarceration_period_release_reason_values,
            name="state_incarceration_period_release_reason"
        ),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="projected_release_reason",
        type_=sa.Enum(
            *new_incarceration_period_release_reason_values,
            name="state_incarceration_period_release_reason"
        ),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_release_reason_old;")


def downgrade():
    # Downgrade incarceration period admission reason for state incarceration period
    op.execute(
        "ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;"
    )
    sa.Enum(
        *old_incarceration_period_admission_reason_values,
        name="state_incarceration_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_incarceration_period_admission_reason_values,
            name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_incarceration_period_admission_reason_values,
            name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_admission_reason_old;")

    # Downgrade incarceration period release reason for state incarceration period (release_reason and projected_release_reason)
    op.execute(
        "ALTER TYPE state_incarceration_period_release_reason RENAME TO state_incarceration_period_release_reason_old;"
    )
    sa.Enum(
        *old_incarceration_period_release_reason_values,
        name="state_incarceration_period_release_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="release_reason",
        type_=sa.Enum(
            *old_incarceration_period_release_reason_values,
            name="state_incarceration_period_release_reason"
        ),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="release_reason",
        type_=sa.Enum(
            *old_incarceration_period_release_reason_values,
            name="state_incarceration_period_release_reason"
        ),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="projected_release_reason",
        type_=sa.Enum(
            *old_incarceration_period_release_reason_values,
            name="state_incarceration_period_release_reason"
        ),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="projected_release_reason",
        type_=sa.Enum(
            *old_incarceration_period_release_reason_values,
            name="state_incarceration_period_release_reason"
        ),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_release_reason_old;")
