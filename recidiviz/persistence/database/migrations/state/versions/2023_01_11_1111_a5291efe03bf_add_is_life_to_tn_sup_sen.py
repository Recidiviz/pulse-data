# pylint: skip-file
"""add_is_life_to_tn_sup_sen

Revision ID: a5291efe03bf
Revises: f383174bc397
Create Date: 2023-01-11 11:11:37.958571

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a5291efe03bf"
down_revision = "79aaffe3c61d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_sentence
        SET is_life = CASE 
                        WHEN SUBSTRING(sentence_metadata, '"LIFETIME_FLAG": "([a-zA-Z0-9_ ]+).*"') IN ('Y','R','IS_LIFE','L','W','H') THEN true 
                        ELSE false 
                        END             
        WHERE state_code = 'US_TN' 
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_sentence
        SET is_life = NULL
        WHERE state_code = 'US_TN' 
        """
    )
