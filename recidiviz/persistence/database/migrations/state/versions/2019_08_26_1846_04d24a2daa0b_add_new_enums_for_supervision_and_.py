# pylint: skip-file
"""add_new_enums_for_supervision_and_incidents

Revision ID: 04d24a2daa0b
Revises: 711e0ee2a7c4
Create Date: 2019-08-26 18:46:04.832059

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "04d24a2daa0b"
down_revision = "711e0ee2a7c4"
branch_labels = None
depends_on = None

# Without new value
old_termination_reason_values = ["ABSCONSION", "DISCHARGE", "REVOCATION", "SUSPENSION"]
old_incident_type_values = [
    "CONTRABAND",
    "DISORDERLY_CONDUCT",
    "ESCAPE",
    "MINOR_OFFENSE",
    "VIOLENCE",
]
old_outcome_type_values = [
    "DISCIPLINARY_LABOR",
    "EXTERNAL_PROSECUTION",
    "FINANCIAL_PENALTY",
    "GOOD_TIME_LOSS",
    "MISCELLANEOUS",
    "NOT_GUILTY",
    "PRIVILEGE_LOSS",
    "SOLITARY",
    "TREATMENT",
    "WARNING",
]

# With new value
new_termination_reason_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONSION",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "REVOCATION",
    "SUSPENSION",
]
new_incident_type_values = [
    "PRESENT_WITHOUT_INFO",
    "CONTRABAND",
    "DISORDERLY_CONDUCT",
    "ESCAPE",
    "MINOR_OFFENSE",
    "POSITIVE",
    "REPORT",
    "VIOLENCE",
]
new_outcome_type_values = [
    "DISCIPLINARY_LABOR",
    "DISMISSED",
    "EXTERNAL_PROSECUTION",
    "FINANCIAL_PENALTY",
    "GOOD_TIME_LOSS",
    "MISCELLANEOUS",
    "NOT_GUILTY",
    "PRIVILEGE_LOSS",
    "SOLITARY",
    "TREATMENT",
    "WARNING",
]


def upgrade() -> None:
    # New state supervision period termination reasons
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

    # New state incarceration incident types
    op.execute(
        "ALTER TYPE state_incarceration_incident_type RENAME TO state_incarceration_incident_type_old;"
    )
    sa.Enum(*new_incident_type_values, name="state_incarceration_incident_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_incident",
        column_name="incident_type",
        type_=sa.Enum(
            *new_incident_type_values, name="state_incarceration_incident_type"
        ),
        postgresql_using="incident_type::text::state_incarceration_incident_type",
    )
    op.alter_column(
        "state_incarceration_incident_history",
        column_name="incident_type",
        type_=sa.Enum(
            *new_incident_type_values, name="state_incarceration_incident_type"
        ),
        postgresql_using="incident_type::text::state_incarceration_incident_type",
    )
    op.execute("DROP TYPE state_incarceration_incident_type_old;")

    # New state incarceration incident outcome types
    op.execute(
        "ALTER TYPE state_incarceration_incident_outcome_type RENAME TO state_incarceration_incident_outcome_type_old;"
    )
    sa.Enum(
        *new_outcome_type_values, name="state_incarceration_incident_outcome_type"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_incident_outcome",
        column_name="outcome_type",
        type_=sa.Enum(
            *new_outcome_type_values, name="state_incarceration_incident_outcome_type"
        ),
        postgresql_using="outcome_type::text::state_incarceration_incident_outcome_type",
    )
    op.alter_column(
        "state_incarceration_incident_outcome_history",
        column_name="outcome_type",
        type_=sa.Enum(
            *new_outcome_type_values, name="state_incarceration_incident_outcome_type"
        ),
        postgresql_using="outcome_type::text::state_incarceration_incident_outcome_type",
    )
    op.execute("DROP TYPE state_incarceration_incident_outcome_type_old;")


def downgrade() -> None:
    # Back to old state incarceration incident outcome types
    op.execute(
        "ALTER TYPE state_incarceration_incident_outcome_type RENAME TO state_incarceration_incident_outcome_type_old;"
    )
    sa.Enum(
        *old_termination_reason_values, name="state_incarceration_incident_outcome_type"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_incident_outcome",
        column_name="outcome_type",
        type_=sa.Enum(
            *old_outcome_type_values, name="state_incarceration_incident_outcome_type"
        ),
        postgresql_using="outcome_type::text::state_incarceration_incident_outcome_type",
    )
    op.alter_column(
        "state_incarceration_incident_outcome_history",
        column_name="outcome_type",
        type_=sa.Enum(
            *old_outcome_type_values, name="state_incarceration_incident_outcome_type"
        ),
        postgresql_using="outcome_type::text::state_incarceration_incident_outcome_type",
    )
    op.execute("DROP TYPE state_incarceration_incident_outcome_type_old;")

    # Back to old state incarceration incident types
    op.execute(
        "ALTER TYPE state_incarceration_incident_type RENAME TO state_incarceration_incident_type_old;"
    )
    sa.Enum(
        *old_termination_reason_values, name="state_incarceration_incident_type"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_incident",
        column_name="incident_type",
        type_=sa.Enum(
            *old_incident_type_values, name="state_incarceration_incident_type"
        ),
        postgresql_using="incident_type::text::state_incarceration_incident_type",
    )
    op.alter_column(
        "state_incarceration_incident_history",
        column_name="incident_type",
        type_=sa.Enum(
            *old_incident_type_values, name="state_incarceration_incident_type"
        ),
        postgresql_using="incident_type::text::state_incarceration_incident_type",
    )
    op.execute("DROP TYPE state_incarceration_incident_type_old;")

    # Back to old state supervision period termination reasons
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
