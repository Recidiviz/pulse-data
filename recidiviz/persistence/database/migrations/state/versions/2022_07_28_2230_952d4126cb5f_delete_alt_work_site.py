# pylint: skip-file
"""delete_alt_work_site

Revision ID: 952d4126cb5f
Revises: 36593ed38a3a
Create Date: 2022-07-28 22:30:20.112263

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "952d4126cb5f"
down_revision = "36593ed38a3a"
branch_labels = None
depends_on = None


# With ALTERNATIVE_WORK_SITE
old_values = [
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
    "ALTERNATIVE_PLACE_OF_EMPLOYMENT",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# Without ALTERNATIVE_WORK_SITE
new_values = [
    "COURT",
    "FIELD",
    "JAIL",
    "PLACE_OF_EMPLOYMENT",
    "RESIDENCE",
    "SUPERVISION_OFFICE",
    "TREATMENT_PROVIDER",
    "LAW_ENFORCEMENT_AGENCY",
    "PAROLE_COMMISSION",
    "ALTERNATIVE_PLACE_OF_EMPLOYMENT",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
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
    op.execute("DROP TYPE state_supervision_contact_location_old;")
