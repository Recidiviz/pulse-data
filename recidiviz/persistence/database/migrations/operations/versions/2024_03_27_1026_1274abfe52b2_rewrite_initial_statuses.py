# pylint: skip-file
"""rewrite_initial_statuses

Revision ID: 1274abfe52b2
Revises: 54384bcdcbb0
Create Date: 2024-03-27 10:26:52.094880

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1274abfe52b2"
down_revision = "a8e2a82b2058"
branch_labels = None
depends_on = None


# IMPORTANT NOTE: THIS MIGRATION WILL NEVER ACTUALLY BE RUN. IT HAS BEEN ADDED BETWEEN
# MIGRATIONS THAT HAVE ALREADY BEEN RUN IN PRODUCTION SO THAT THE
# test_direct_ingest_instance_status_contains_data_for_all_states MIGRATION TEST
# PASSES.
def upgrade() -> None:
    op.execute(
        "UPDATE direct_ingest_instance_status "
        "SET status = 'INITIAL_STATE' "
        "WHERE status = 'STANDARD_RERUN_STARTED';"
    )
    op.execute(
        "UPDATE direct_ingest_instance_status "
        "SET status = 'NO_RAW_DATA_REIMPORT_IN_PROGRESS' "
        "WHERE status = 'NO_RERUN_IN_PROGRESS';"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE direct_ingest_instance_status "
        "SET status = 'STANDARD_RERUN_STARTED' "
        "WHERE status = 'INITIAL_STATE';"
    )
    op.execute(
        "UPDATE direct_ingest_instance_status "
        "SET status = 'NO_RERUN_IN_PROGRESS' "
        "WHERE status = 'NO_RAW_DATA_REIMPORT_IN_PROGRESS';"
    )
