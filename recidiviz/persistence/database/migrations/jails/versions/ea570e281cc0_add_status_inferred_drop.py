"""add_status_inferred_drop

Revision ID: ea570e281cc0
Revises: c679a1773b4c
Create Date: 2019-02-04 14:49:01.445654

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'ea570e281cc0'
down_revision = 'c679a1773b4c'
branch_labels = None
depends_on = None

# Without new value
old_values = ['ACQUITTED', 'COMPLETED_SENTENCE', 'CONVICTED', 'DROPPED',
              'EXTERNAL_UNKNOWN', 'PENDING', 'PRETRIAL', 'SENTENCED',
              'UNKNOWN_FOUND_IN_SOURCE', 'UNKNOWN_REMOVED_FROM_SOURCE']

# With new value
new_values = ['ACQUITTED', 'COMPLETED_SENTENCE', 'CONVICTED', 'DROPPED',
              'EXTERNAL_UNKNOWN', 'INFERRED_DROPPED', 'PENDING', 'PRETRIAL',
              'SENTENCED', 'UNKNOWN_FOUND_IN_SOURCE',
              'UNKNOWN_REMOVED_FROM_SOURCE']


def upgrade():
    op.execute('ALTER TYPE charge_status RENAME TO charge_status_old;')
    sa.Enum(*new_values, name='charge_status').create(bind=op.get_bind())
    op.alter_column('charge', column_name='status',
                    type_=sa.Enum(*new_values, name='charge_status'),
                    postgresql_using='status::text::charge_status')
    op.alter_column('charge_history', column_name='status',
                    type_=sa.Enum(*new_values, name='charge_status'),
                    postgresql_using='status::text::charge_status')
    op.execute('DROP TYPE charge_status_old;')


def downgrade():
    op.execute('ALTER TYPE charge_status RENAME TO charge_status_old;')
    sa.Enum(*old_values, name='charge_status').create(bind=op.get_bind())
    op.alter_column('charge', column_name='status',
                    type_=sa.Enum(*old_values, name='charge_status'),
                    postgresql_using='status::text::charge_status')
    op.alter_column('charge_history', column_name='status',
                    type_=sa.Enum(*old_values, name='charge_status'),
                    postgresql_using='status::text::charge_status')
    op.execute('DROP TYPE charge_status_old;')
