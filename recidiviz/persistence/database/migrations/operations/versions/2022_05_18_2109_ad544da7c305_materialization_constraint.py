# pylint: skip-file
"""materialization_constraint

Revision ID: ad544da7c305
Revises: 8b7d65e34984
Create Date: 2022-05-18 21:09:48.205336

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ad544da7c305"
down_revision = "8b7d65e34984"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "job_times_ordering", "direct_ingest_view_materialization_metadata"
    )
    op.create_check_constraint(
        "job_times_ordering",
        "direct_ingest_view_materialization_metadata",
        "materialization_time IS NULL OR materialization_time >= job_creation_time",
    )


def downgrade() -> None:
    op.drop_constraint(
        "job_times_ordering", "direct_ingest_view_materialization_metadata"
    )
    op.create_check_constraint(
        "job_times_ordering",
        "direct_ingest_view_materialization_metadata",
        "materialization_time IS NULL OR materialization_time > job_creation_time",
    )
