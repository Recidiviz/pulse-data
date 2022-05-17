# pylint: skip-file
"""rename_status_table

Revision ID: 38483f72dca7
Revises: 09ae7387fcbd
Create Date: 2022-05-16 15:55:40.525052

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "38483f72dca7"
down_revision = "09ae7387fcbd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rename `direct_ingest_instance_status` to `direct_ingest_instance_pause_status` and update indices and
    # constraints.
    op.drop_constraint(
        "single_row_per_ingest_instance",
        "direct_ingest_instance_status",
        type_="unique",
    )
    op.execute(
        "ALTER TABLE direct_ingest_instance_status RENAME TO direct_ingest_instance_pause_status;"
    )
    op.execute(
        "ALTER INDEX direct_ingest_instance_status_pkey RENAME TO "
        "direct_ingest_instance_pause_status_pkey"
    )
    op.execute(
        "ALTER INDEX ix_direct_ingest_instance_status_instance RENAME TO "
        "ix_direct_ingest_instance_pause_status_instance"
    )
    op.execute(
        "ALTER INDEX ix_direct_ingest_instance_status_region_code RENAME TO "
        "ix_direct_ingest_instance_pause_status_region_code"
    )
    op.create_unique_constraint(
        "single_row_per_ingest_instance",
        "direct_ingest_instance_pause_status",
        ["region_code", "instance"],
    )


def downgrade() -> None:
    # To downgrade, rename `direct_ingest_instance_pause_status` to `direct_ingest_instance_status` and update indices
    # and constraints.
    op.drop_constraint(
        "single_row_per_ingest_instance",
        "direct_ingest_instance_pause_status",
        type_="unique",
    )
    op.execute(
        "ALTER TABLE direct_ingest_instance_pause_status RENAME TO direct_ingest_instance_status;"
    )
    op.execute(
        "ALTER INDEX direct_ingest_instance_pause_status_pkey RENAME TO "
        "direct_ingest_instance_status_pkey"
    )
    op.execute(
        "ALTER INDEX ix_direct_ingest_instance_pause_status_instance RENAME TO "
        "ix_direct_ingest_instance_status_instance"
    )
    op.execute(
        "ALTER INDEX ix_direct_ingest_instance_pause_status_region_code RENAME TO "
        "ix_direct_ingest_instance_status_region_code"
    )

    op.create_unique_constraint(
        "single_row_per_ingest_instance",
        "direct_ingest_instance_status",
        ["region_code", "instance"],
    )
