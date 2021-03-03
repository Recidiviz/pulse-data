"""add_new_incarceration_enum_values

Revision ID: 86b7d2889a0e
Revises: 7ab233533ebe
Create Date: 2019-09-12 20:57:47.232109

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "86b7d2889a0e"
down_revision = "7ab233533ebe"
branch_labels = None
depends_on = None

# Without new value
old_incarceration_type_values = ["COUNTY_JAIL", "STATE_PRISON"]
old_incarceration_period_admission_reason_values = [
    "ADMITTED_IN_ERROR",
    "EXTERNAL_UNKNOWN",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "TRANSFER",
]
old_incarceration_period_release_reason_values = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "ESCAPE",
    "EXTERNAL_UNKNOWN",
    "RELEASED_IN_ERROR",
    "SENTENCE_SERVED",
    "TRANSFER",
]

# With new value
new_incarceration_type_values = ["COUNTY_JAIL", "EXTERNAL_UNKNOWN", "STATE_PRISON"]
new_incarceration_period_admission_reason_values = [
    "ADMITTED_IN_ERROR",
    "EXTERNAL_UNKNOWN",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
]
new_incarceration_period_release_reason_values = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "ESCAPE",
    "EXTERNAL_UNKNOWN",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "SENTENCE_SERVED",
    "TRANSFER",
]


def upgrade():
    # Update incarceration type for state incarceration period and state incarceration sentence
    op.execute(
        "ALTER TYPE state_incarceration_type RENAME TO state_incarceration_type_old;"
    )
    sa.Enum(*new_incarceration_type_values, name="state_incarceration_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="incarceration_type",
        type_=sa.Enum(*new_incarceration_type_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="incarceration_type",
        type_=sa.Enum(*new_incarceration_type_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_sentence",
        column_name="incarceration_type",
        type_=sa.Enum(*new_incarceration_type_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_sentence_history",
        column_name="incarceration_type",
        type_=sa.Enum(*new_incarceration_type_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.execute("DROP TYPE state_incarceration_type_old;")

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
    # Downgrade incarceration type for state incarceration period and state incarceration sentence
    op.execute(
        "ALTER TYPE state_incarceration_type RENAME TO state_incarceration_type_old;"
    )
    sa.Enum(*old_incarceration_type_values, name="state_incarceration_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="incarceration_type",
        type_=sa.Enum(*old_incarceration_type_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="incarceration_type",
        type_=sa.Enum(*old_incarceration_type_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_sentence",
        column_name="incarceration_type",
        type_=sa.Enum(*old_incarceration_type_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_sentence_history",
        column_name="incarceration_type",
        type_=sa.Enum(*old_incarceration_type_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.execute("DROP TYPE state_incarceration_type_old;")

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
