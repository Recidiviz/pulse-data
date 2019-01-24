"""add_ChargeDegree_FOURTH

Revision ID: 3bf06134321e
Revises: 328448d2b4d2
Create Date: 2019-01-07 15:22:41.921960

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3bf06134321e'
down_revision = '328448d2b4d2'
branch_labels = None
depends_on = None

# Without 'FOURTH'
old_enum_values = ['FIRST', 'SECOND', 'THIRD']
# With 'FOURTH'
new_enum_values = ['FIRST', 'SECOND', 'THIRD', 'FOURTH']


def upgrade():
    op.execute('ALTER TYPE charge_degree RENAME TO charge_degree_old;')
    sa.Enum(*new_enum_values, name='charge_degree').create(bind=op.get_bind())
    op.alter_column('charge', column_name='degree',
                    type_=sa.Enum(*new_enum_values, name='charge_degree'),
                    postgresql_using='degree::text::charge_degree')
    op.alter_column('charge_history', column_name='degree',
                    type_=sa.Enum(*new_enum_values, name='charge_degree'),
                    postgresql_using='degree::text::charge_degree')
    op.execute('DROP TYPE charge_degree_old;')


def downgrade():
    op.execute('ALTER TYPE charge_degree RENAME TO charge_degree_old;')
    sa.Enum(*old_enum_values, name='charge_degree').create(bind=op.get_bind())
    op.alter_column('charge', column_name='degree',
                    type_=sa.Enum(*old_enum_values, name='charge_degree'),
                    postgresql_using='degree::text::charge_degree')
    op.alter_column('charge_history', column_name='degree',
                    type_=sa.Enum(*old_enum_values, name='charge_degree'),
                    postgresql_using='degree::text::charge_degree')
    op.execute('DROP TYPE charge_degree_old;')
