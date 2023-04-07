# pylint: skip-file
"""update_mi_custodial_authority

Revision ID: 99807d598d3e
Revises: 84b3f4639723
Create Date: 2023-04-07 10:51:29.938637

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "99807d598d3e"
down_revision = "84b3f4639723"
branch_labels = None
depends_on = None

# Currently, all supervision periods are mapped (with a literal enum) to custodial authority = SUPERVISION_AUTHORITY unless they're an out of state location.
# This migration maps periods with supervision site involving US MARSHALS to FEDERAL and supervision site OUTSIDE MDOC JURISDICTION to INTERNAL_UNKNOWN.

UPGRADE_QUERY = """
    UPDATE state_supervision_period
    SET custodial_authority_raw_text = supervision_site,
        custodial_authority = 'FEDERAL'
    WHERE state_code = 'US_MI' and supervision_site in ('UNITED STATES MARSHALLS', 'US MARSHAL SERVICE');

    UPDATE state_supervision_period
    SET custodial_authority_raw_text = supervision_site,
        custodial_authority = 'INTERNAL_UNKNOWN'
    WHERE state_code = 'US_MI' and supervision_site = 'OUTSIDE MDOC JURISDICTION';   
"""

DOWNGRADE_QUERY = """
    UPDATE state_supervision_period
    SET custodial_authority_raw_text = NULL,
        custodial_authority = 'SUPERVISION_AUTHORITY'
    WHERE state_code = 'US_MI' and supervision_site in ('UNITED STATES MARSHALLS', 'US MARSHAL SERVICE');

    UPDATE state_supervision_period
    SET custodial_authority_raw_text = NULL,
        custodial_authority = 'SUPERVISION_AUTHORITY'
    WHERE state_code = 'US_MI' and supervision_site = 'OUTSIDE MDOC JURISDICTION'; 
"""


def upgrade() -> None:
    op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
