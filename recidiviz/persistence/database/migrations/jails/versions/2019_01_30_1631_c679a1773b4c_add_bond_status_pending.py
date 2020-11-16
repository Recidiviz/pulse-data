"""add_bond_status_pending

Revision ID: c679a1773b4c
Revises: c1d537485b56
Create Date: 2019-01-30 16:31:37.731753

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'c679a1773b4c'
down_revision = 'c1d537485b56'
branch_labels = None
depends_on = None

# Without 'PENDING'
old_values = ['DENIED', 'UNKNOWN_FOUND_IN_SOURCE', 'INFERRED_SET',
              'NOT_REQUIRED', 'POSTED', 'UNKNOWN_REMOVED_FROM_SOURCE', 'SET']

# With 'CIVIL'
new_values = ['DENIED', 'UNKNOWN_FOUND_IN_SOURCE', 'INFERRED_SET',
              'NOT_REQUIRED', 'PENDING', 'POSTED',
              'UNKNOWN_REMOVED_FROM_SOURCE', 'SET']


def upgrade():
    op.execute('ALTER TYPE bond_status RENAME TO bond_status_old;')
    sa.Enum(*new_values, name='bond_status').create(bind=op.get_bind())
    op.alter_column('bond', column_name='status',
                    type_=sa.Enum(*new_values, name='bond_status'),
                    postgresql_using='status::text::bond_status')
    op.alter_column('bond_history', column_name='status',
                    type_=sa.Enum(*new_values, name='bond_status'),
                    postgresql_using='status::text::bond_status')
    op.execute('DROP TYPE bond_status_old;')


def downgrade():
    op.execute('ALTER TYPE bond_status RENAME TO bond_status_old;')
    sa.Enum(*old_values, name='bond_status').create(bind=op.get_bind())
    op.alter_column('bond', column_name='status',
                    type_=sa.Enum(*old_values, name='bond_status'),
                    postgresql_using='status::text::bond_status')
    op.alter_column('bond_history', column_name='status',
                    type_=sa.Enum(*old_values, name='bond_status'),
                    postgresql_using='status::text::bond_status')
    op.execute('DROP TYPE bond_status_old;')
