# pylint: skip-file
"""adjust_id_sup_sent

Revision ID: 244b56c165ac
Revises: 5ddf1ef08d76
Create Date: 2022-09-07 16:09:57.040720

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "244b56c165ac"
down_revision = "5ddf1ef08d76"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE state_charge SET is_violent = FALSE WHERE state_code = 'US_ID' AND is_violent IS NULL;"
    )
    op.execute(
        "UPDATE state_charge SET is_sex_offense = FALSE WHERE state_code = 'US_ID' AND is_sex_offense IS NULL;"
    )
    op.execute(
        "UPDATE state_supervision_sentence SET supervision_type_raw_text = NULL WHERE state_code = 'US_ID' AND supervision_type = 'PROBATION';"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE state_charge SET is_violent = NULL WHERE state_code = 'US_ID' AND is_violent = FALSE;"
    )
    op.execute(
        "UPDATE state_charge SET is_sex_offense = NULL WHERE state_code = 'US_ID' AND is_sex_offense = FALSE;"
    )
    op.execute(
        "UPDATE state_supervision_sentence SET supervision_type_raw_text = 'PROBATION' WHERE state_code = 'US_ID' AND supervision_type = 'PROBATION';"
    )
