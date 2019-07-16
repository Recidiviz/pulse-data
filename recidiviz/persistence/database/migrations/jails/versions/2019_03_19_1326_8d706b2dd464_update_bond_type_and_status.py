"""update_bond_type_and_status

Revision ID: 8d706b2dd464
Revises: 2ec8a00921c9
Create Date: 2019-03-19 13:26:50.738616

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8d706b2dd464'
down_revision = '2ec8a00921c9'
branch_labels = None
depends_on = None

old_bond_status_values = ['DENIED', 'PRESENT_WITHOUT_INFO', 'INFERRED_SET', 'NOT_REQUIRED', 'PENDING', 'POSTED', 'REVOKED', 'REMOVED_WITHOUT_INFO', 'SET']
old_bond_type_values = ['EXTERNAL_UNKNOWN', 'CASH', 'NO_BOND', 'PARTIAL_CASH', 'SECURED', 'REMOVED_WITHOUT_INFO', 'UNSECURED']

new_bond_status_values = ['PENDING', 'POSTED', 'PRESENT_WITHOUT_INFO', 'REMOVED_WITHOUT_INFO', 'REVOKED', 'SET']
new_bond_type_values = ['CASH', 'DENIED', 'EXTERNAL_UNKNOWN', 'NOT_REQUIRED', 'PARTIAL_CASH', 'SECURED', 'UNSECURED']

def upgrade():
    op.execute('ALTER TYPE bond_status RENAME TO bond_status_old;')
    sa.Enum(*new_bond_status_values, name='bond_status').create(bind=op.get_bind())
    op.alter_column('bond', column_name='status',
                    type_=sa.Enum(*new_bond_status_values, name='bond_status'),
                    postgresql_using='status::text::bond_status')
    op.alter_column('bond_history', column_name='status',
                    type_=sa.Enum(*new_bond_status_values, name='bond_status'),
                    postgresql_using='status::text::bond_status')
    op.execute('DROP TYPE bond_status_old;')

    op.execute('ALTER TYPE bond_type RENAME TO bond_type_old;')
    sa.Enum(*new_bond_type_values, name='bond_type').create(bind=op.get_bind())
    op.alter_column('bond', column_name='bond_type',
                    type_=sa.Enum(*new_bond_type_values, name='bond_type'),
                    postgresql_using='bond_type::text::bond_type')
    op.alter_column('bond_history', column_name='bond_type',
                    type_=sa.Enum(*new_bond_type_values, name='bond_type'),
                    postgresql_using='bond_type::text::bond_type')
    op.execute('DROP TYPE bond_type_old;')


def downgrade():
    op.execute('ALTER TYPE bond_status RENAME TO bond_status_old;')
    sa.Enum(*old_bond_status_values, name='bond_status').create(bind=op.get_bind())
    op.alter_column('bond', column_name='status',
                    type_=sa.Enum(*old_bond_status_values, name='bond_status'),
                    postgresql_using='status::text::bond_status')
    op.alter_column('bond_history', column_name='status',
                    type_=sa.Enum(*old_bond_status_values, name='bond_status'),
                    postgresql_using='status::text::bond_status')
    op.execute('DROP TYPE bond_status_old;')

    op.execute('ALTER TYPE bond_type RENAME TO bond_type_old;')
    sa.Enum(*old_bond_type_values, name='bond_type').create(bind=op.get_bind())
    op.alter_column('bond', column_name='bond_type',
                    type_=sa.Enum(*old_bond_type_values, name='bond_type'),
                    postgresql_using='bond_type::text::bond_type')
    op.alter_column('bond_history', column_name='bond_type',
                    type_=sa.Enum(*old_bond_type_values, name='bond_type'),
                    postgresql_using='bond_type::text::bond_type')
    op.execute('DROP TYPE bond_type_old;')
