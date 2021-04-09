# pylint: skip-file
"""informal_probation_and_custodial_authority

Revision ID: 4b9155409eb1
Revises: 7fcbb12b8f34
Create Date: 2020-05-29 14:18:51.617958

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "4b9155409eb1"
down_revision = "7fcbb12b8f34"
branch_labels = None
depends_on = None

old_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "INVESTIGATION",
    "PAROLE",
    "PROBATION",
    "DUAL",
]
new_values = [
    "EXTERNAL_UNKNOWN",
    "INFORMAL_PROBATION",
    "INTERNAL_UNKNOWN",
    "INVESTIGATION",
    "PAROLE",
    "PROBATION",
    "DUAL",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_supervision_type RENAME TO state_supervision_period_supervision_type_old;"
    )
    sa.Enum(*new_values, name="state_supervision_period_supervision_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_period_supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_period_supervision_type::text::state_supervision_period_supervision_type",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_period_supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_period_supervision_type::text::state_supervision_period_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_period_supervision_type_old;")

    op.add_column(
        "state_incarceration_period",
        sa.Column("custodial_authority", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "state_incarceration_period_history",
        sa.Column("custodial_authority", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "state_supervision_period",
        sa.Column("custodial_authority", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "state_supervision_period_history",
        sa.Column("custodial_authority", sa.String(length=255), nullable=True),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_supervision_type RENAME TO state_supervision_period_supervision_type_old;"
    )
    sa.Enum(*old_values, name="state_supervision_period_supervision_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_period_supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_period_supervision_type::text::state_supervision_period_supervision_type",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_period_supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_period_supervision_type::text::state_supervision_period_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_period_supervision_type_old;")

    op.drop_column("state_supervision_period_history", "custodial_authority")
    op.drop_column("state_supervision_period", "custodial_authority")
    op.drop_column("state_incarceration_period_history", "custodial_authority")
    op.drop_column("state_incarceration_period", "custodial_authority")
    # ### end Alembic commands ###
