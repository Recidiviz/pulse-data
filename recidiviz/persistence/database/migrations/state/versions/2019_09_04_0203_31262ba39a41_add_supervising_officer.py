# pylint: skip-file
"""add_supervising_officer

Revision ID: 31262ba39a41
Revises: 8610ff2ed64b
Create Date: 2019-09-04 02:03:11.600423

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '31262ba39a41'
down_revision = '8610ff2ed64b'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('state_supervision_period', sa.Column('supervising_officer_id', sa.Integer(), nullable=True))
    op.create_foreign_key('state_supervision_period_supervising_officer_id_fkey', 'state_supervision_period', 'state_agent', ['supervising_officer_id'], ['agent_id'])
    op.add_column('state_supervision_period_history', sa.Column('supervising_officer_id', sa.Integer(), nullable=True))
    op.create_foreign_key('state_supervision_period_history_supervising_officer_id_fkey', 'state_supervision_period_history', 'state_agent', ['supervising_officer_id'], ['agent_id'])


def downgrade():
    op.drop_constraint('state_supervision_period_history_supervising_officer_id_fkey', 'state_supervision_period_history', type_='foreignkey')
    op.drop_column('state_supervision_period_history', 'supervising_officer_id')
    op.drop_constraint('state_supervision_period_supervising_officer_id_fkey', 'state_supervision_period', type_='foreignkey')
    op.drop_column('state_supervision_period', 'supervising_officer_id')
