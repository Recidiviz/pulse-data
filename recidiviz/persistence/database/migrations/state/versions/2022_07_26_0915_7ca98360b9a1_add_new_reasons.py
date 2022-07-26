# pylint: skip-file
"""add_new_reasons

Revision ID: 7ca98360b9a1
Revises: f304039e3c04
Create Date: 2022-07-26 09:15:36.103962

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7ca98360b9a1"
down_revision = "f304039e3c04"
branch_labels = None
depends_on = None

new_admission_reasons = [
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

old_admission_reasons = [
    "ADMITTED_IN_ERROR",
    "ADMITTED_FROM_SUPERVISION",
    "NEW_ADMISSION",
    "REVOCATION",
    "RETURN_FROM_ERRONEOUS_RELEASE",
    "RETURN_FROM_ESCAPE",
    "RETURN_FROM_TEMPORARY_RELEASE",
    "SANCTION_ADMISSION",
    "STATUS_CHANGE",
    "TEMPORARY_CUSTODY",
    "TRANSFER",
    "TRANSFER_FROM_OTHER_JURISDICTION",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

new_release_reasons = [
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

old_release_reasons = [
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
    "TEMPORARY_RELEASE",
    "SENTENCE_SERVED",
    "STATUS_CHANGE",
    "TRANSFER",
    "TRANSFER_TO_OTHER_JURISDICTION",
    "VACATED",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;"
    )
    sa.Enum(
        *new_admission_reasons, name="state_incarceration_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *new_admission_reasons, name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_admission_reason_old;")

    op.execute(
        "ALTER TYPE state_incarceration_period_release_reason RENAME TO state_incarceration_period_release_reason_old;"
    )
    sa.Enum(
        *new_release_reasons, name="state_incarceration_period_release_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="release_reason",
        type_=sa.Enum(
            *new_release_reasons, name="state_incarceration_period_release_reason"
        ),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_release_reason_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;"
    )
    sa.Enum(
        *old_admission_reasons, name="state_incarceration_period_admission_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="admission_reason",
        type_=sa.Enum(
            *old_admission_reasons, name="state_incarceration_period_admission_reason"
        ),
        postgresql_using="admission_reason::text::state_incarceration_period_admission_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_admission_reason_old;")

    op.execute(
        "ALTER TYPE state_incarceration_period_release_reason RENAME TO state_incarceration_period_release_reason_old;"
    )
    sa.Enum(
        *old_release_reasons, name="state_incarceration_period_release_reason"
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="release_reason",
        type_=sa.Enum(
            *old_release_reasons, name="state_incarceration_period_release_reason"
        ),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_release_reason_old;")
