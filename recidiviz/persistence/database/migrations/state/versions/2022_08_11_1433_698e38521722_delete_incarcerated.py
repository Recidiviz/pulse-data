# pylint: skip-file
"""delete_incarcerated

Revision ID: 698e38521722
Revises: b23f1567bce5
Create Date: 2022-08-11 14:33:09.720619

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "698e38521722"
down_revision = "b23f1567bce5"
branch_labels = None
depends_on = None


# With INCARCERATED
old_values = [
    "MINIMUM",
    "MEDIUM",
    "HIGH",
    "MAXIMUM",
    "IN_CUSTODY",
    "INCARCERATED",
    "DIVERSION",
    "INTERSTATE_COMPACT",
    "ELECTRONIC_MONITORING_ONLY",
    "LIMITED",
    "UNSUPERVISED",
    "UNASSIGNED",
    "PRESENT_WITHOUT_INFO",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# Without INCARCERATED
new_values = [
    "MINIMUM",
    "MEDIUM",
    "HIGH",
    "MAXIMUM",
    "IN_CUSTODY",
    "DIVERSION",
    "INTERSTATE_COMPACT",
    "ELECTRONIC_MONITORING_ONLY",
    "LIMITED",
    "UNSUPERVISED",
    "UNASSIGNED",
    "PRESENT_WITHOUT_INFO",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_level RENAME TO state_supervision_level_old;"
    )
    sa.Enum(*new_values, name="state_supervision_level").create(bind=op.get_bind())

    op.alter_column(
        "state_supervision_period",
        column_name="supervision_level",
        type_=sa.Enum(*new_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )

    op.execute("DROP TYPE state_supervision_level_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_level RENAME TO state_supervision_level_old;"
    )
    sa.Enum(*old_values, name="state_supervision_level").create(bind=op.get_bind())

    op.alter_column(
        "state_supervision_period",
        column_name="supervision_level",
        type_=sa.Enum(*old_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )

    op.execute("DROP TYPE state_supervision_level_old;")
