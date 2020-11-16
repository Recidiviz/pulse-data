"""add_bond_status_revoked

Revision ID: 61e3adfaab5e
Revises: 4e8b3d90d337
Create Date: 2019-02-07 17:13:56.291683

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '61e3adfaab5e'
down_revision = '4e8b3d90d337'
branch_labels = None
depends_on = None

# Without new value
old_values = ['DENIED','UNKNOWN_FOUND_IN_SOURCE','INFERRED_SET','NOT_REQUIRED','PENDING','POSTED','UNKNOWN_REMOVED_FROM_SOURCE','SET']
new_values = ['DENIED','UNKNOWN_FOUND_IN_SOURCE','INFERRED_SET','NOT_REQUIRED','PENDING','POSTED','REVOKED','UNKNOWN_REMOVED_FROM_SOURCE','SET']

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
