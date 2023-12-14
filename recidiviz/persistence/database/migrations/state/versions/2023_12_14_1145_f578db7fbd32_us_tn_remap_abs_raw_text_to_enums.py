# pylint: skip-file
"""us_tn_remap_abs_raw_text_to_enums

Revision ID: f578db7fbd32
Revises: b5305c88e6da
Create Date: 2023-12-14 11:45:02.291140

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f578db7fbd32"
down_revision = "b5305c88e6da"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'ABSCONSION'
        WHERE
            state_code = 'US_TN'
            and supervision_level = 'INTERNAL_UNKNOWN'
            and supervision_level_raw_text in ('9AB', 'ABS', 'ZAB', 'ZAC', 'ZAP', 'NIA');

        UPDATE state_supervision_period
        SET supervision_type = 'ABSCONSION'
        WHERE
            state_code = 'US_TN'
            and supervision_type = 'BENCH_WARRANT'
            and supervision_type_raw_text like 'NIA%';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'INTERNAL_UNKNOWN'
        WHERE
            state_code = 'US_TN'
            and supervision_level = 'ABSCONSION'
            and supervision_level_raw_text in ('9AB', 'ABS', 'ZAB', 'ZAC', 'ZAP', 'NIA');

        UPDATE state_supervision_period
        SET supervision_type = 'BENCH_WARRANT'
        WHERE
            state_code = 'US_TN'
            and supervision_type = 'ABSCONSION'
            and supervision_type_raw_text like 'NIA%';
        """
    )
