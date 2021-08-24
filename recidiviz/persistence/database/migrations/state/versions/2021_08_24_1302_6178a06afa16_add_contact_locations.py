# pylint: skip-file
"""add_contact_locations

Revision ID: 6178a06afa16
Revises: 3433a0b16898
Create Date: 2021-08-24 13:02:28.353361

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6178a06afa16"
down_revision = "3433a0b16898"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "COURT",
    "FIELD",
    "JAIL",
    "PLACE_OF_EMPLOYMENT",
    "RESIDENCE",
    "SUPERVISION_OFFICE",
    "TREATMENT_PROVIDER",
    "LAW_ENFORCEMENT_AGENCY",
]

# With new value
new_values = [
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "COURT",
    "FIELD",
    "JAIL",
    "PLACE_OF_EMPLOYMENT",
    "RESIDENCE",
    "SUPERVISION_OFFICE",
    "TREATMENT_PROVIDER",
    "LAW_ENFORCEMENT_AGENCY",
    "PAROLE_COMMISSION",
    "ALTERNATIVE_WORK_SITE",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_contact_location RENAME TO state_supervision_contact_location_old;"
    )
    sa.Enum(*new_values, name="state_supervision_contact_location").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_contact",
        column_name="location",
        type_=sa.Enum(*new_values, name="state_supervision_contact_location"),
        postgresql_using="location::text::state_supervision_contact_location",
    )
    op.alter_column(
        "state_supervision_contact_history",
        column_name="location",
        type_=sa.Enum(*new_values, name="state_supervision_contact_location"),
        postgresql_using="location::text::state_supervision_contact_location",
    )
    op.execute("DROP TYPE state_supervision_contact_location_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_contact_location RENAME TO state_supervision_contact_location_old;"
    )
    sa.Enum(*old_values, name="state_supervision_contact_location").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_contact",
        column_name="location",
        type_=sa.Enum(*old_values, name="state_supervision_contact_location"),
        postgresql_using="location::text::state_supervision_contact_location",
    )
    op.alter_column(
        "state_supervision_contact_history",
        column_name="location",
        type_=sa.Enum(*old_values, name="state_supervision_contact_location"),
        postgresql_using="location::text::state_supervision_contact_location",
    )
    op.execute("DROP TYPE state_supervision_contact_location_old;")
