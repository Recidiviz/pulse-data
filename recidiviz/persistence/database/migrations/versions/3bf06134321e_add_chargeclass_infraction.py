"""add_ChargeClass_INFRACTION

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

# Without 'INFRACTION'
old_enum_values = ['CIVIL', 'EXTERNAL_UNKNOWN', 'FELONY', 'MISDEMEANOR',
                   'PAROLE_VIOLATION', 'PROBATION_VIOLATION']
# With 'INFRACTION'
new_enum_values = ['CIVIL', 'EXTERNAL_UNKNOWN', 'FELONY', 'INFRACTION',
                   'MISDEMEANOR', 'PAROLE_VIOLATION', 'PROBATION_VIOLATION']

def upgrade():
    op.execute('ALTER TYPE charge_class RENAME TO charge_class_old;')
    sa.Enum(*new_enum_values, name='charge_class').create(bind=op.get_bind())
    op.alter_column('charge', column_name='class',
                    type_=sa.Enum(*new_enum_values, name='charge_class'),
                    postgresql_using='class::text::charge_class')
    op.alter_column('charge_history', column_name='class',
                    type_=sa.Enum(*new_enum_values, name='charge_class'),
                    postgresql_using='class::text::charge_class')
    op.execute('DROP TYPE charge_class_old;')


def downgrade():
    op.execute('ALTER TYPE charge_class RENAME TO charge_class_old;')
    sa.Enum(*old_enum_values, name='charge_class').create(bind=op.get_bind())
    op.alter_column('charge', column_name='class',
                    type_=sa.Enum(*old_enum_values, name='charge_class'),
                    postgresql_using='class::text::charge_class')
    op.alter_column('charge_history', column_name='class', 
                    type_=sa.Enum(*old_enum_values, name='charge_class'),
                    postgresql_using='class::text::charge_class')
    op.execute('DROP TYPE charge_class_old;')
