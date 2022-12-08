# pylint: skip-file
"""drop_and_rebuild_violated_condition_entry

Revision ID: 80c6f79b69ab
Revises: 7e70b52d2720
Create Date: 2022-12-05 19:53:19.460011

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "80c6f79b69ab"
down_revision = "7e70b52d2720"
branch_labels = None
depends_on = None

INSERT_PA_QUERY = """
    INSERT INTO state_supervision_violated_condition_entry (state_code, condition_raw_text, person_id, supervision_violation_id)
    SELECT state_code, violation_type_raw_text, person_id, supervision_violation_id
    FROM state_supervision_violation_type_entry
    WHERE state_code = 'US_PA'
"""

DROP_PA_QUERY = """
    DELETE FROM state_supervision_violated_condition_entry
    WHERE state_code = 'US_PA'
"""


def upgrade() -> None:
    op.execute(DROP_PA_QUERY)
    op.execute(INSERT_PA_QUERY)


def downgrade() -> None:
    pass
