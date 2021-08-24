# pylint: skip-file
"""add_contact_types

Revision ID: 3433a0b16898
Revises: 7cbf1ade1343
Create Date: 2021-08-23 16:23:14.613401

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3433a0b16898"
down_revision = "7cbf1ade1343"
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
]

# With new value
new_values = [
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "FACE_TO_FACE",
    "TELEPHONE",
    "WRITTEN_MESSAGE",
    "VIRTUAL",
    "DIRECT",
    "COLLATERAL",
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
