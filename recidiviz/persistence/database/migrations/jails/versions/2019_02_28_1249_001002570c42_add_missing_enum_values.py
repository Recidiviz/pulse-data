"""add_missing_enum_values

Revision ID: 001002570c42
Revises: cfc3a453f53f
Create Date: 2019-02-28 12:49:34.934276

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '001002570c42'
down_revision = 'cfc3a453f53f'
branch_labels = None
depends_on = None

# Without new values
old_values = ['EXTERNAL_UNKNOWN', 'CASH', 'NO_BOND', 'PARTIAL_CASH', 'SECURED', 'UNSECURED']

# With new value
new_values = ['EXTERNAL_UNKNOWN', 'CASH', 'NO_BOND', 'PARTIAL_CASH', 'SECURED', 'UNKNOWN_REMOVED_FROM_SOURCE', 'UNSECURED']

def upgrade():
    op.execute('ALTER TYPE bond_type RENAME TO bond_type_old;')
    sa.Enum(*new_values, name='bond_type').create(bind=op.get_bind())
    op.alter_column('bond', column_name='bond_type',
                    type_=sa.Enum(*new_values, name='bond_type'),
                    postgresql_using='bond_type::text::bond_type')
    op.alter_column('bond_history', column_name='bond_type',
                    type_=sa.Enum(*new_values, name='bond_type'),
                    postgresql_using='bond_type::text::bond_type')
    op.execute('DROP TYPE bond_type_old;')


def downgrade():
    op.execute('ALTER TYPE bond_type RENAME TO bond_type_old;')
    sa.Enum(*old_values, name='bond_type').create(bind=op.get_bind())
    op.alter_column('bond', column_name='bond_type',
                    type_=sa.Enum(*old_values, name='bond_type'),
                    postgresql_using='bond_type::text::bond_type')
    op.alter_column('bond_history', column_name='bond_type',
                    type_=sa.Enum(*old_values, name='bond_type'),
                    postgresql_using='bond_type::text::bond_type')
    op.execute('DROP TYPE bond_type_old;')
