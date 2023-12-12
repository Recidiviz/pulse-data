# pylint: skip-file
"""us_mi_warrant_status

Revision ID: 09f505a2a4c1
Revises: 159c8a262bc6
Create Date: 2023-12-11 16:29:26.023935

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "09f505a2a4c1"
down_revision = "159c8a262bc6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_type = 'WARRANT_STATUS'
        WHERE
            state_code = 'US_MI'
            AND supervision_type = 'BENCH_WARRANT'
            AND supervision_type_raw_text = 'WARRANT-SUPERVISION_LEVEL-2286';

        UPDATE state_supervision_period
        SET supervision_type = 'ABSCONSION'
        WHERE
            state_code = 'US_MI'
            AND supervision_type = 'BENCH_WARRANT'
            AND supervision_type_raw_text = 'WARRANT-SUPERVISION_LEVEL-2394';

        UPDATE state_supervision_period
        SET supervision_level = 'ABSCONSION'
        WHERE
            state_code = 'US_MI'
            AND supervision_level = 'WARRANT'
            AND supervision_level_raw_text LIKE '2394%';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_type = 'BENCH_WARRANT'
        WHERE
            state_code = 'US_MI'
            AND supervision_type = 'WARRANT_STATUS'
            AND supervision_type_raw_text = 'WARRANT-SUPERVISION_LEVEL-2286';

        UPDATE state_supervision_period
        SET supervision_type = 'BENCH_WARRANT'
        WHERE
            state_code = 'US_MI'
            AND supervision_type = 'ABSCONSION'
            AND supervision_type_raw_text = 'WARRANT-SUPERVISION_LEVEL-2394';

        UPDATE state_supervision_period
        SET supervision_level = 'WARRANT'
        WHERE
            state_code = 'US_MI'
            AND supervision_level = 'ABSCONSION'
            AND supervision_level_raw_text LIKE '2394%';
        """
    )
