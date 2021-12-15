# pylint: skip-file
"""add_law_enforcement_agency

Revision ID: 9a623575162e
Revises: 2989a2836e23
Create Date: 2021-05-07 10:42:04.728745

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9a623575162e"
down_revision = "2989a2836e23"
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
