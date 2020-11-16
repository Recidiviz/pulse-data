"""add_chargedegree_fourth

Revision ID: 1103917126e8
Revises: 3bf06134321e
Create Date: 2019-01-24 14:40:53.349876

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1103917126e8'
down_revision = '3bf06134321e'
branch_labels = None
depends_on = None

# Without 'FOURTH'
old_enum_values = ['EXTERNAL_UNKNOWN', 'FIRST', 'SECOND', 'THIRD']
# With FOURTH
new_enum_values = ['EXTERNAL_UNKNOWN', 'FIRST', 'SECOND', 'THIRD', 'FOURTH']


def upgrade():
    op.execute('ALTER TYPE degree RENAME TO degree_old;')
    sa.Enum(*new_enum_values, name='degree').create(bind=op.get_bind())
    op.alter_column('charge', column_name='degree',
                    type_=sa.Enum(*new_enum_values, name='degree'),
                    postgresql_using='degree::text::degree')
    op.alter_column('charge_history', column_name='degree',
                    type_=sa.Enum(*new_enum_values, name='degree'),
                    postgresql_using='degree::text::degree')
    op.execute('DROP TYPE degree_old;')


def downgrade():
    op.execute('ALTER TYPE degree RENAME TO degree_old;')
    sa.Enum(*old_enum_values, name='degree').create(bind=op.get_bind())
    op.alter_column('charge', column_name='degree',
                    type_=sa.Enum(*old_enum_values, name='degree'),
                    postgresql_using='degree::text::degree')
    op.alter_column('charge_history', column_name='degree',
                    type_=sa.Enum(*old_enum_values, name='degree'),
                    postgresql_using='degree::text::degree')
    op.execute('DROP TYPE degree_old;')
