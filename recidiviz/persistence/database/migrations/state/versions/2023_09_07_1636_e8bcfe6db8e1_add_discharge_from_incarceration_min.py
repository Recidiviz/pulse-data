# pylint: skip-file
"""add_discharge_from_incarceration_min

Revision ID: e8bcfe6db8e1
Revises: 0b6c094c5447
Create Date: 2023-09-07 16:36:22.679739

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e8bcfe6db8e1"
down_revision = "f15234a8978b"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "APPEAL_FOR_TRANSFER_TO_SUPERVISION_FROM_INCARCERATION",
    "ARREST_CHECK",
    "SUPERVISION_CASE_PLAN_UPDATE",
    "DISCHARGE_EARLY_FROM_SUPERVISION",
    "DISCHARGE_FROM_INCARCERATION",
    "DISCHARGE_FROM_SUPERVISION",
    "DRUG_SCREEN",
    "EMPLOYMENT_VERIFICATION",
    "FACE_TO_FACE_CONTACT",
    "HOME_VISIT",
    "NEW_ASSESSMENT",
    "PAYMENT_VERIFICATION",
    "SPECIAL_CONDITION_VERIFICATION",
    "TRANSFER_TO_ADMINISTRATIVE_SUPERVISION",
    "TRANSFER_TO_SUPERVISION_FROM_INCARCERATION",
    "TREATMENT_REFERRAL",
    "TREATMENT_VERIFICATION",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# With new value
new_values = [
    "APPEAL_FOR_TRANSFER_TO_SUPERVISION_FROM_INCARCERATION",
    "ARREST_CHECK",
    "SUPERVISION_CASE_PLAN_UPDATE",
    "DISCHARGE_EARLY_FROM_SUPERVISION",
    "DISCHARGE_FROM_INCARCERATION",
    "DISCHARGE_FROM_SUPERVISION",
    "DRUG_SCREEN",
    "EMPLOYMENT_VERIFICATION",
    "FACE_TO_FACE_CONTACT",
    "HOME_VISIT",
    "NEW_ASSESSMENT",
    "PAYMENT_VERIFICATION",
    "SPECIAL_CONDITION_VERIFICATION",
    "TRANSFER_TO_ADMINISTRATIVE_SUPERVISION",
    "TRANSFER_TO_SUPERVISION_FROM_INCARCERATION",
    "TREATMENT_REFERRAL",
    "TREATMENT_VERIFICATION",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "DISCHARGE_FROM_INCARCERATION_MIN",
]


def upgrade() -> None:
    op.execute("ALTER TYPE state_task_type RENAME TO state_task_type_old;")
    sa.Enum(*new_values, name="state_task_type").create(bind=op.get_bind())
    op.alter_column(
        "state_task_deadline",
        column_name="task_type",
        type_=sa.Enum(*new_values, name="state_task_type"),
        postgresql_using="task_type::text::state_task_type",
    )
    op.execute("DROP TYPE state_task_type_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_task_type RENAME TO state_task_type_old;")
    sa.Enum(*old_values, name="state_task_type").create(bind=op.get_bind())
    op.alter_column(
        "state_task_deadline",
        column_name="task_type",
        type_=sa.Enum(*old_values, name="state_task_type"),
        postgresql_using="task_type::text::state_task_type",
    )
    op.execute("DROP TYPE state_task_type_old;")
