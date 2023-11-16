# pylint: skip-file
"""update_tn_response_type_for_ztvr_viols

Revision ID: e2bdea9f99ad
Revises: f537606bf82f
Create Date: 2023-11-15 11:59:07.838219

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e2bdea9f99ad"
down_revision = "f537606bf82f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_violation_response
        SET response_type = 'CITATION'
        WHERE 
            state_code = 'US_TN' 
            and violation_response_metadata::jsonb->>'SanctionStatus' = 'ASSOCIATED TO ZERO TOLERANCE';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_violation_response
        SET response_type = NULL
        WHERE 
            state_code = 'US_TN' 
            and violation_response_metadata::jsonb->>'SanctionStatus' = 'ASSOCIATED TO ZERO TOLERANCE';
        """
    )
