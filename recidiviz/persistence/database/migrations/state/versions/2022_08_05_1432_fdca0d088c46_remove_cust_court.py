# pylint: skip-file
"""remove_cust_court

Revision ID: fdca0d088c46
Revises: 32d171c066ec
Create Date: 2022-08-05 14:32:26.533896

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fdca0d088c46"
down_revision = "32d171c066ec"
branch_labels = None
depends_on = None


# With COURT
old_values = [
    "COURT",
    "COUNTY",
    "FEDERAL",
    "OTHER_COUNTRY",
    "OTHER_STATE",
    "SUPERVISION_AUTHORITY",
    "STATE_PRISON",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# Without COURT
new_values = [
    "COUNTY",
    "FEDERAL",
    "OTHER_COUNTRY",
    "OTHER_STATE",
    "SUPERVISION_AUTHORITY",
    "STATE_PRISON",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_custodial_authority RENAME TO state_custodial_authority_old;"
    )
    sa.Enum(*new_values, name="state_custodial_authority").create(bind=op.get_bind())

    op.alter_column(
        "state_incarceration_period",
        column_name="custodial_authority",
        type_=sa.Enum(*new_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )
    op.alter_column(
        "state_supervision_period",
        column_name="custodial_authority",
        type_=sa.Enum(*new_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )

    op.execute("DROP TYPE state_custodial_authority_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_custodial_authority RENAME TO state_custodial_authority_old;"
    )
    sa.Enum(*old_values, name="state_custodial_authority").create(bind=op.get_bind())

    op.alter_column(
        "state_incarceration_period",
        column_name="custodial_authority",
        type_=sa.Enum(*old_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )
    op.alter_column(
        "state_supervision_period",
        column_name="custodial_authority",
        type_=sa.Enum(*old_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )

    op.execute("DROP TYPE state_custodial_authority_old;")
