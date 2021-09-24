# pylint: skip-file
"""pa_residency_status

Revision ID: c5e0b7ec2b9a
Revises: cd0e4833e102
Create Date: 2021-09-20 17:23:37.698259

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c5e0b7ec2b9a"
down_revision = "cd0e4833e102"
branch_labels = None
depends_on = None


UPDATE_QUERY = """
UPDATE {table_name}
SET residency_status = 'HOMELESS'
WHERE state_code = 'US_PA' AND residency_status in (
  'TRANSIENT'
);
"""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            "state_person",
            "state_person_history",
        ]:
            op.execute(
                UPDATE_QUERY.format(
                    table_name=table_name,
                )
            )


def downgrade() -> None:
    # No downgrade, this is a lossy migration
    pass
