# pylint: skip-file
"""add_supervision_levels

Revision ID: 6f9e81083732
Revises: 21bfb057577a
Create Date: 2022-08-24 10:50:37.424669

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6f9e81083732"
down_revision = "21bfb057577a"
branch_labels = None
depends_on = None

# Without the added values
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

# With added values
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
    "WARRANT",
    "ABSCONSION",
    "INTAKE",
    "RESIDENTIAL_PROGRAM",
    "FURLOUGH",
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
