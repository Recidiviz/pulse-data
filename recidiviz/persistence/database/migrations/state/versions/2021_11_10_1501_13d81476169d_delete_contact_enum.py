# pylint: skip-file
"""delete_contact_enum

Revision ID: 13d81476169d
Revises: 369961fa537b
Create Date: 2021-11-10 15:01:33.923267

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "13d81476169d"
down_revision = "369961fa537b"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "FACE_TO_FACE",
    "TELEPHONE",
    "WRITTEN_MESSAGE",
    "VIRTUAL",
    "COLLATERAL",
    "DIRECT",
    "BOTH_COLLATERAL_AND_DIRECT",
]

# With new value
new_values = [
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "COLLATERAL",
    "DIRECT",
    "BOTH_COLLATERAL_AND_DIRECT",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_contact_type RENAME TO state_supervision_contact_type_old;"
    )
    sa.Enum(*new_values, name="state_supervision_contact_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_contact",
        column_name="contact_type",
        type_=sa.Enum(*new_values, name="state_supervision_contact_type"),
        postgresql_using="contact_type::text::state_supervision_contact_type",
    )
    op.alter_column(
        "state_supervision_contact_history",
        column_name="contact_type",
        type_=sa.Enum(*new_values, name="state_supervision_contact_type"),
        postgresql_using="contact_type::text::state_supervision_contact_type",
    )
    op.execute("DROP TYPE state_supervision_contact_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_contact_type RENAME TO state_supervision_contact_type_old;"
    )
    sa.Enum(*old_values, name="state_supervision_contact_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_contact",
        column_name="contact_type",
        type_=sa.Enum(*old_values, name="state_supervision_contact_type"),
        postgresql_using="contact_type::text::state_supervision_contact_type",
    )
    op.alter_column(
        "state_supervision_contact_history",
        column_name="contact_type",
        type_=sa.Enum(*old_values, name="state_supervision_contact_type"),
        postgresql_using="contact_type::text::state_supervision_contact_type",
    )
    op.execute("DROP TYPE state_supervision_contact_type_old;")
