"""add_bond_type_partial_cash

Revision ID: 6b23f60e636c
Revises: 61e3adfaab5e
Create Date: 2019-02-08 14:47:07.048315

"""

# Uncomment the below block if the migration needs to import from any child
# package of recidiviz, otherwise delete

# # Hackity hack to get around the fact that alembic runs this file as a
# # top-level module rather than a child of the recidiviz module
# import sys
# import os
# module_path = os.path.abspath(__file__)
# # Walk up directories to reach main package
# while not module_path.split('/')[-1] == 'recidiviz':
#     if module_path == '/':
#         raise RuntimeError("Top-level recidiviz package not found")
#     module_path = os.path.dirname(module_path)
# # Must insert parent directory of main package
# sys.path.insert(0, os.path.dirname(module_path))

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6b23f60e636c'
down_revision = '61e3adfaab5e'
branch_labels = None
depends_on = None

# Without new value
old_values = ['EXTERNAL_UNKNOWN','CASH','NO_BOND','SECURED','UNSECURED']
new_values = ['EXTERNAL_UNKNOWN','CASH','NO_BOND','PARTIAL_CASH', 'SECURED','UNSECURED']

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
