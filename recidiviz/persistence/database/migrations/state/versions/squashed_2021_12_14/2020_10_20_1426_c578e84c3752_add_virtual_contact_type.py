# pylint: skip-file
"""add_virtual_contact_type

Revision ID: c578e84c3752
Revises: 0002e286d896
Create Date: 2020-10-20 14:26:48.922902

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c578e84c3752"
down_revision = "0002e286d896"
branch_labels = None
depends_on = None

# Without `VIRTUAL`
old_values = [
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "FACE_TO_FACE",
    "TELEPHONE",
    "WRITTEN_MESSAGE",
]

# With `VIRTUAL`
new_values = [
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "FACE_TO_FACE",
    "TELEPHONE",
    "WRITTEN_MESSAGE",
    "VIRTUAL",
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
