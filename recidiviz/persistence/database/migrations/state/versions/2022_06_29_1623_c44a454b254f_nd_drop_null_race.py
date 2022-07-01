# pylint: skip-file
"""nd_drop_null_race

Revision ID: c44a454b254f
Revises: 51d22e2b9b32
Create Date: 2022-06-29 18:03:01.481394

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "c44a454b254f"
down_revision = "51d22e2b9b32"
branch_labels = None
depends_on = None

DELETE_QUERY = (
    "DELETE FROM state_person_race WHERE state_code = 'US_ND' AND race IS NULL;"
)


def upgrade() -> None:
    op.execute(
        StrictStringFormatter().format(
            DELETE_QUERY,
        )
    )


def downgrade() -> None:
    # This migration does not have a downgrade
    pass
