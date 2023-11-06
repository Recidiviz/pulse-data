# pylint: skip-file
"""add_wknd_conf_enums

Revision ID: 8b3e4ce45334
Revises: 018e184b8b4e
Create Date: 2023-11-02 13:14:33.818700

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8b3e4ce45334"
down_revision = "018e184b8b4e"
branch_labels = None
depends_on = None

# Incarceration period admission reasons, with WEEKEND_CONFINEMENT
new_ip_admission_reasons = [
    "ADMITTED_IN_ERROR",
    "ADMITTED_FROM_SUPERVISION",
    "ESCAPE",
    "NEW_ADMISSION",
    "REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "RETURN_FROM_TEMPORARY_RELEASE",
    "SANCTION_ADMISSION",
    "STATUS_CHANGE",
    "TEMPORARY_CUSTODY",
    "TEMPORARY_RELEASE",
    "TRANSFER",
    "TRANSFER_FROM_OTHER_JURISDICTION",
    "WEEKEND_CONFINEMENT",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# Incarceration period admission reasons, without WEEKEND_CONFINEMENT
old_ip_admission_reasons = [
    "ADMITTED_IN_ERROR",
    "ADMITTED_FROM_SUPERVISION",
    "ESCAPE",
    "NEW_ADMISSION",
    "REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "RETURN_FROM_TEMPORARY_RELEASE",
    "SANCTION_ADMISSION",
    "STATUS_CHANGE",
    "TEMPORARY_CUSTODY",
    "TEMPORARY_RELEASE",
    "TRANSFER",
    "TRANSFER_FROM_OTHER_JURISDICTION",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# Incarceration period release reasons, with RELEASE_FROM_WEEKEND_CONFINEMENT
new_ip_release_reasons = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "ESCAPE",
    "EXECUTION",
    "PARDONED",
    "RELEASED_FROM_ERRONEOUS_ADMISSION",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "RELEASED_TO_SUPERVISION",
    "RELEASE_FROM_WEEKEND_CONFINEMENT",
    "RETURN_FROM_ESCAPE",
    "RETURN_FROM_TEMPORARY_RELEASE",
    "TEMPORARY_RELEASE",
    "SENTENCE_SERVED",
    "STATUS_CHANGE",
    "TRANSFER",
    "TRANSFER_TO_OTHER_JURISDICTION",
    "VACATED",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# Incarceration period release reasons, without RELEASE_FROM_WEEKEND_CONFINEMENT
old_ip_release_reasons = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "ESCAPE",
    "EXECUTION",
    "PARDONED",
    "RELEASED_FROM_ERRONEOUS_ADMISSION",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "RELEASED_TO_SUPERVISION",
    "RETURN_FROM_ESCAPE",
    "RETURN_FROM_TEMPORARY_RELEASE",
    "TEMPORARY_RELEASE",
    "SENTENCE_SERVED",
    "STATUS_CHANGE",
    "TRANSFER",
    "TRANSFER_TO_OTHER_JURISDICTION",
    "VACATED",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# Supervision period admission reasons, with RETURN_FROM_WEEKEND_CONFINEMENT
new_sp_admission_reasons = [
    "ABSCONSION",
    "RELEASE_FROM_INCARCERATION",
    "COURT_SENTENCE",
    "INVESTIGATION",
    "TRANSFER_FROM_OTHER_JURISDICTION",
    "TRANSFER_WITHIN_STATE",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
    "RETURN_FROM_WEEKEND_CONFINEMENT",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# Supervision period admission reasons, without RETURN_FROM_WEEKEND_CONFINEMENT
old_sp_admission_reasons = [
    "ABSCONSION",
    "RELEASE_FROM_INCARCERATION",
    "COURT_SENTENCE",
    "INVESTIGATION",
    "TRANSFER_FROM_OTHER_JURISDICTION",
    "TRANSFER_WITHIN_STATE",
    "RETURN_FROM_ABSCONSION",
    "RETURN_FROM_SUSPENSION",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# Supervision period termination reasons, with WEEKEND_CONFINEMENT
new_sp_termination_reasons = [
    "ABSCONSION",
    "ADMITTED_TO_INCARCERATION",
    "COMMUTED",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "INVESTIGATION",
    "PARDONED",
    "TRANSFER_TO_OTHER_JURISDICTION",
    "TRANSFER_WITHIN_STATE",
    "RETURN_FROM_ABSCONSION",
    "REVOCATION",
    "SUSPENSION",
    "VACATED",
    "WEEKEND_CONFINEMENT",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# Supervision period termination reasons, without WEEKEND_CONFINEMENT
old_sp_termination_reasons = [
    "ABSCONSION",
    "ADMITTED_TO_INCARCERATION",
    "COMMUTED",
    "DEATH",
    "DISCHARGE",
    "EXPIRATION",
    "INVESTIGATION",
    "PARDONED",
    "TRANSFER_TO_OTHER_JURISDICTION",
    "TRANSFER_WITHIN_STATE",
    "RETURN_FROM_ABSCONSION",
    "REVOCATION",
    "SUSPENSION",
    "VACATED",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]


def upgrade() -> None:
    # Incarceration period admission/release reasons
    op.execute(
        "ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;"
    )
    sa.Enum(
        *new_ip_admission_reasons, name="state_incarceration_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_ip_admission_reasons,
            name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_admission_reason_old;")

    op.execute(
        "ALTER TYPE state_incarceration_period_release_reason RENAME TO state_incarceration_period_release_reason_old;"
    )
    sa.Enum(
        *new_ip_release_reasons, name="state_incarceration_period_release_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="release_reason",
        type_=sa.Enum(
            *new_ip_release_reasons, name="state_incarceration_period_release_reason"
        ),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_release_reason_old;")

    # Supervision period admission/termination reasons
    op.execute(
        "ALTER TYPE state_supervision_period_admission_reason RENAME TO state_supervision_period_admission_reason_old;"
    )
    sa.Enum(
        *new_sp_admission_reasons, name="state_supervision_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_sp_admission_reasons, name="state_supervision_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.execute("DROP TYPE state_supervision_period_admission_reason_old;")

    op.execute(
        "ALTER TYPE state_supervision_period_termination_reason RENAME TO state_supervision_period_termination_reason_old;"
    )
    sa.Enum(
        *new_sp_termination_reasons, name="state_supervision_period_termination_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="termination_reason",
        type_=sa.Enum(
            *new_sp_termination_reasons,
            name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")


def downgrade() -> None:
    # Incarceration period admission/release reasons
    op.execute(
        "ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;"
    )
    sa.Enum(
        *old_ip_admission_reasons, name="state_incarceration_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_ip_admission_reasons,
            name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_admission_reason_old;")

    op.execute(
        "ALTER TYPE state_incarceration_period_release_reason RENAME TO state_incarceration_period_release_reason_old;"
    )
    sa.Enum(
        *old_ip_release_reasons, name="state_incarceration_period_release_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="release_reason",
        type_=sa.Enum(
            *old_ip_release_reasons, name="state_incarceration_period_release_reason"
        ),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_release_reason_old;")

    # Supervision period admission/termination reasons
    op.execute(
        "ALTER TYPE state_supervision_period_admission_reason RENAME TO state_supervision_period_admission_reason_old;"
    )
    sa.Enum(
        *old_sp_admission_reasons, name="state_supervision_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_sp_admission_reasons, name="state_supervision_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_supervision_period_admission_reason",
    )
    op.execute("DROP TYPE state_supervision_period_admission_reason_old;")

    op.execute(
        "ALTER TYPE state_supervision_period_termination_reason RENAME TO state_supervision_period_termination_reason_old;"
    )
    sa.Enum(
        *old_sp_termination_reasons, name="state_supervision_period_termination_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="termination_reason",
        type_=sa.Enum(
            *old_sp_termination_reasons,
            name="state_supervision_period_termination_reason"
        ),
        postgresql_using="termination_reason::text::state_supervision_period_termination_reason",
    )
    op.execute("DROP TYPE state_supervision_period_termination_reason_old;")
