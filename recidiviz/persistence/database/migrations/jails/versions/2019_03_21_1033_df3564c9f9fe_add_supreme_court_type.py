"""add_supreme_court_type

Revision ID: df3564c9f9fe
Revises: 8d706b2dd464
Create Date: 2019-03-21 10:33:30.542742

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'df3564c9f9fe'
down_revision = '8d706b2dd464'
branch_labels = None
depends_on = None

# Without new value
old_values = ['CIRCUIT', 'CIVIL', 'DISTRICT', 'EXTERNAL_UNKNOWN', 'OTHER', 'SUPERIOR']

# With new value
new_values = ['CIRCUIT', 'CIVIL', 'DISTRICT', 'EXTERNAL_UNKNOWN', 'OTHER', 'SUPERIOR', 'SUPREME']

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
