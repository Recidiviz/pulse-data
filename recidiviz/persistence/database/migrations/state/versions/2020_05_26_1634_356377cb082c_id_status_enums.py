# pylint: skip-file
"""id_status_enums

Revision ID: 356377cb082c
Revises: fbb8c36dd4b9
Create Date: 2020-05-26 16:34:54.810013

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
from sqlalchemy.dialects import postgresql

revision = "356377cb082c"
down_revision = "fbb8c36dd4b9"
branch_labels = None
depends_on = None

# Without new value
old_supervision_termination_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONSION",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "INVESTIGATION",
    "RETURN_FROM_ABSCONSION",
    "REVOCATION",
    "SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TRANSFER_WITHIN_STATE",
]
old_incarceration_admission_values = [
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
old_specialized_purpose_for_incarceration_values = [
    "SHOCK_INCARCERATION",
    "TREATMENT_IN_PRISON",
]

# With new value
new_supervision_termination_values = [
    "EXTERNAL_UNKNOWN",
    "ABSCONSION",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "INVESTIGATION",
    "RETURN_FROM_ABSCONSION",
    "RETURN_TO_INCARCERATION",
    "REVOCATION",
    "SUSPENSION",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TRANSFER_WITHIN_STATE",
]
new_incarceration_admission_values = [
    "ADMITTED_IN_ERROR",
    "DUAL_REVOCATION",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "NEW_ADMISSION",
    "PAROLE_REVOCATION",
    "PROBATION_REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "RETURN_FROM_SUPERVISION",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
    "TRANSFERRED_FROM_OUT_OF_STATE",
]
new_specialized_purpose_for_incarceration_values = [
    "GENERAL",
    "PAROLE_BOARD_HOLD",
    "SHOCK_INCARCERATION",
    "TREATMENT_IN_PRISON",
]


def upgrade():
    # Add new supervision period termination values
    op.execute(
        "ALTER TYPE state_supervision_period_termination_reason RENAME TO state_supervision_period_termination_reason_old;"
    )
    sa.Enum(
        *new_supervision_termination_values,
        name="state_supervision_period_termination_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="termination_reason",
        type_=sa.Enum(
            *new_supervision_termination_values,
            name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="termination_reason",
        type_=sa.Enum(
            *new_supervision_termination_values,
            name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")

    # Add new incarceration period admission values
    op.execute(
        "ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;"
    )
    sa.Enum(
        *new_incarceration_admission_values,
        name="state_incarceration_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_incarceration_admission_values,
            name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_incarceration_admission_values,
            name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_admission_reason_old;")

    # Add new incarceration period specialized purpose for incarceration
    op.execute(
        "ALTER TYPE state_specialized_purpose_for_incarceration RENAME TO state_specialized_purpose_for_incarceration_old;"
    )
    sa.Enum(
        *new_specialized_purpose_for_incarceration_values,
        name="state_specialized_purpose_for_incarceration"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="specialized_purpose_for_incarceration",
        type_=sa.Enum(
            *new_specialized_purpose_for_incarceration_values,
            name="state_specialized_purpose_for_incarceration"
        ),
        postgresql_using="specialized_purpose_for_incarceration::text::state_specialized_purpose_for_incarceration",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="specialized_purpose_for_incarceration",
        type_=sa.Enum(
            *new_specialized_purpose_for_incarceration_values,
            name="state_specialized_purpose_for_incarceration"
        ),
        postgresql_using="specialized_purpose_for_incarceration::text::state_specialized_purpose_for_incarceration",
    )
    op.execute("DROP TYPE state_specialized_purpose_for_incarceration_old;")

    # Add StateSupervisionPeriod.supervision_period_supervision_type and enum
    state_supervision_period_supervision_type = postgresql.ENUM(
        "EXTERNAL_UNKNOWN",
        "INTERNAL_UNKNOWN",
        "INVESTIGATION",
        "PAROLE",
        "PROBATION",
        "DUAL",
        name="state_supervision_period_supervision_type",
    )
    state_supervision_period_supervision_type.create(op.get_bind())
    op.add_column(
        "state_supervision_period",
        sa.Column(
            "supervision_period_supervision_type",
            sa.Enum(
                "EXTERNAL_UNKNOWN",
                "INTERNAL_UNKNOWN",
                "INVESTIGATION",
                "PAROLE",
                "PROBATION",
                "DUAL",
                name="state_supervision_period_supervision_type",
            ),
            nullable=True,
        ),
    )
    op.add_column(
        "state_supervision_period",
        sa.Column(
            "supervision_period_supervision_type_raw_text",
            sa.String(length=255),
            nullable=True,
        ),
    )
    op.add_column(
        "state_supervision_period_history",
        sa.Column(
            "supervision_period_supervision_type",
            sa.Enum(
                "EXTERNAL_UNKNOWN",
                "INTERNAL_UNKNOWN",
                "INVESTIGATION",
                "PAROLE",
                "PROBATION",
                "DUAL",
                name="state_supervision_period_supervision_type",
            ),
            nullable=True,
        ),
    )
    op.add_column(
        "state_supervision_period_history",
        sa.Column(
            "supervision_period_supervision_type_raw_text",
            sa.String(length=255),
            nullable=True,
        ),
    )
    # ### end Alembic commands ###


def downgrade():
    # Remove new supervision period termination values
    op.execute(
        "ALTER TYPE state_supervision_period_termination_reason RENAME TO state_supervision_period_termination_reason_old;"
    )
    sa.Enum(
        *old_supervision_termination_values,
        name="state_supervision_period_termination_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="termination_reason",
        type_=sa.Enum(
            *old_supervision_termination_values,
            name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="termination_reason",
        type_=sa.Enum(
            *old_supervision_termination_values,
            name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")

    # Remove new incarceration period admission values
    op.execute(
        "ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;"
    )
    sa.Enum(
        *old_incarceration_admission_values,
        name="state_incarceration_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_incarceration_admission_values,
            name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_incarceration_admission_values,
            name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_admission_reason_old;")

    # Remove new specialized purpose for incarceration values
    op.execute(
        "ALTER TYPE state_specialized_purpose_for_incarceration RENAME TO state_specialized_purpose_for_incarceration_old;"
    )
    sa.Enum(
        *old_specialized_purpose_for_incarceration_values,
        name="state_specialized_purpose_for_incarceration"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="specialized_purpose_for_incarceration",
        type_=sa.Enum(
            *old_specialized_purpose_for_incarceration_values,
            name="state_specialized_purpose_for_incarceration"
        ),
        postgresql_using="specialized_purpose_for_incarceration::text::state_specialized_purpose_for_incarceration",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="specialized_purpose_for_incarceration",
        type_=sa.Enum(
            *old_specialized_purpose_for_incarceration_values,
            name="state_specialized_purpose_for_incarceration"
        ),
        postgresql_using="specialized_purpose_for_incarceration::text::state_specialized_purpose_for_incarceration",
    )
    op.execute("DROP TYPE state_specialized_purpose_for_incarceration_old;")

    # Remove StateSupervisionPeriod.supervision_period_supervision_type and enum
    op.drop_column(
        "state_supervision_period_history",
        "supervision_period_supervision_type_raw_text",
    )
    op.drop_column(
        "state_supervision_period_history", "supervision_period_supervision_type"
    )
    op.drop_column(
        "state_supervision_period", "supervision_period_supervision_type_raw_text"
    )
    op.drop_column("state_supervision_period", "supervision_period_supervision_type")
    state_supervision_period_supervision_type = postgresql.ENUM(
        "EXTERNAL_UNKNOWN",
        "INTERNAL_UNKNOWN",
        "INVESTIGATION",
        "PAROLE",
        "PROBATION",
        "DUAL",
        name="state_supervision_period_supervision_type",
    )
    state_supervision_period_supervision_type.drop(op.get_bind())
    # ### end Alembic commands ###
