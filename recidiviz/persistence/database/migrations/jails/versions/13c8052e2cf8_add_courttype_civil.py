"""add_courttype_civil

Revision ID: 13c8052e2cf8
Revises: 93693ee54c46
Create Date: 2019-01-28 10:38:58.635265

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '13c8052e2cf8'
down_revision = '93693ee54c46'
branch_labels = None
depends_on = None


# Without 'CIVIL'
old_values = ['CIRCUIT', 'DISTRICT', 'EXTERNAL_UNKNOWN', 'OTHER', 'SUPERIOR']

# With 'CIVIL'
new_values = ['CIRCUIT', 'CIVIL', 'DISTRICT', 'EXTERNAL_UNKNOWN', 'OTHER',
              'SUPERIOR']


def upgrade():
    op.execute('ALTER TYPE court_type RENAME TO court_type_old;')
    sa.Enum(*new_values, name='court_type').create(bind=op.get_bind())
    op.alter_column('charge', column_name='court_type',
                    type_=sa.Enum(*new_values, name='court_type'),
                    postgresql_using='court_type::text::court_type')
    op.alter_column('charge_history', column_name='court_type',
                    type_=sa.Enum(*new_values, name='court_type'),
                    postgresql_using='court_type::text::court_type')
    op.execute('DROP TYPE court_type_old;')


def downgrade():
    op.execute('ALTER TYPE court_type RENAME TO court_type_old;')
    sa.Enum(*old_values, name='court_type').create(bind=op.get_bind())
    op.alter_column('charge', column_name='court_type',
                    type_=sa.Enum(*old_values, name='court_type'),
                    postgresql_using='court_type::text::court_type')
    op.alter_column('charge_history', column_name='court_type', 
                    type_=sa.Enum(*old_values, name='court_type'),
                    postgresql_using='court_type::text::court_type')
    op.execute('DROP TYPE court_type_old;')
