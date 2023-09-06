# pylint: skip-file
"""add_sex_offense_parser

Revision ID: 0b6c094c5447
Revises: d8ba40be25d1
Create Date: 2023-09-06 15:49:17.673539

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0b6c094c5447"
down_revision = "d8ba40be25d1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_charge
        SET is_sex_offense =
            CASE
                WHEN statute IN (
                    'I18-1505B',
                    'I18-1505B(1)',
                    'I18-1506',
                    'I18-1506(1)(A)',
                    'I18-1506(1)(B)',
                    'I18-1506(1)(C)',
                    'I18-1506(AT)',
                    'I18-1507',
                    'I18-1508 {AT}',
                    'I18-1508 {F}',
                    'I18-1508',
                    'I18-1508A(1)(A)',
                    'I18-1508A(1)(C)',
                    'I18-4502',
                    'I18-6101 (AB)',
                    'I18-6101 {A}',
                    'I18-6101 {AT}',
                    'I18-6101-1 {F}',
                    'I18-6101',
                    'I18-6101(1)',
                    'I18-6101(2)',
                    'I18-6101(3)',
                    'I18-6101(4)',
                    'I18-6101(5)',
                    'I18-6108',
                    'I18-6602',
                    'I18-6605',
                    'I18-6606 (AT)',
                    'I18-6606',
                    'I18-6609(3)',
                    'I18-8304',
                    'I18-8307',
                    'I18-8308',
                    'I18-924',
                    'I18-925',
                    'I19-2520C'
                ) THEN TRUE
                ELSE FALSE
            END
        WHERE 
            state_code = 'US_IX';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_charge
        SET is_sex_offense = NULL
        WHERE
            state_code = 'US_IX';
        """
    )
