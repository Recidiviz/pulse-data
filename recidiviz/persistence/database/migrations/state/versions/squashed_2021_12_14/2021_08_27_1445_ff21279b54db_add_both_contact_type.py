# pylint: skip-file
"""add_both_contact_type

Revision ID: ff21279b54db
Revises: 4e3749fd0305
Create Date: 2021-08-27 14:45:55.364635

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ff21279b54db"
down_revision = "4e3749fd0305"
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
    "DIRECT",
    "COLLATERAL",
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
