"""multiple_enum_updates

Revision ID: a0e74f9019c8
Revises: 5b4387de078f
Create Date: 2019-03-22 17:26:25.631867

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a0e74f9019c8'
down_revision = '5b4387de078f'
branch_labels = None
depends_on = None

# Without new value
old_gender_values = ['EXTERNAL_UNKNOWN', 'FEMALE', 'MALE', 'OTHER', 'TRANS_FEMALE', 'TRANS_MALE']
old_admission_reason_values = ['ESCAPE', 'NEW_COMMITMENT', 'PAROLE_VIOLATION', 'PROBATION_VIOLATION', 'TRANSFER'] 
old_charge_class_values = ['CIVIL', 'EXTERNAL_UNKNOWN', 'FELONY', 'INFRACTION', 'MISDEMEANOR', 'OTHER', 'PAROLE_VIOLATION', 'PROBATION_VIOLATION']

# With new value
new_gender_values = ['EXTERNAL_UNKNOWN', 'FEMALE', 'MALE', 'OTHER', 'TRANS', 'TRANS_FEMALE', 'TRANS_MALE']
new_admission_reason_values = ['ESCAPE', 'NEW_COMMITMENT', 'PAROLE_VIOLATION', 'PROBATION_VIOLATION', 'SUPERVISION_VIOLATION_FOR_SEX_OFFENSE', 'TRANSFER'] 
new_charge_class_values = ['CIVIL', 'EXTERNAL_UNKNOWN', 'FELONY', 'INFRACTION', 'MISDEMEANOR', 'OTHER', 'PAROLE_VIOLATION', 'PROBATION_VIOLATION', 'SUPERVISION_VIOLATION_FOR_SEX_OFFENSE']

def upgrade():
    op.execute('ALTER TYPE gender RENAME TO gender_old;')
    sa.Enum(*new_gender_values, name='gender').create(bind=op.get_bind())
    op.alter_column('person', column_name='gender',
                    type_=sa.Enum(*new_gender_values, name='gender'),
                    postgresql_using='gender::text::gender')
    op.alter_column('person_history', column_name='gender',
                    type_=sa.Enum(*new_gender_values, name='gender'),
                    postgresql_using='gender::text::gender')
    op.execute('DROP TYPE gender_old;')

    op.execute('ALTER TYPE admission_reason RENAME TO admission_reason_old;')
    sa.Enum(*new_admission_reason_values, name='admission_reason').create(bind=op.get_bind())
    op.alter_column('booking', column_name='admission_reason',
                    type_=sa.Enum(*new_admission_reason_values, name='admission_reason'),
                    postgresql_using='admission_reason::text::admission_reason')
    op.alter_column('booking_history', column_name='admission_reason',
                    type_=sa.Enum(*new_admission_reason_values, name='admission_reason'),
                    postgresql_using='admission_reason::text::admission_reason')
    op.execute('DROP TYPE admission_reason_old;')

    op.execute('ALTER TYPE charge_class RENAME TO charge_class_old;')
    sa.Enum(*new_charge_class_values, name='charge_class').create(bind=op.get_bind())
    op.alter_column('charge', column_name='class',
                    type_=sa.Enum(*new_charge_class_values, name='charge_class'),
                    postgresql_using='class::text::charge_class')
    op.alter_column('charge_history', column_name='class',
                    type_=sa.Enum(*new_charge_class_values, name='charge_class'),
                    postgresql_using='class::text::charge_class')
    op.execute('DROP TYPE charge_class_old;')


def downgrade():
    op.execute('ALTER TYPE gender RENAME TO gender_old;')
    sa.Enum(*old_gender_values, name='gender').create(bind=op.get_bind())
    op.alter_column('person', column_name='gender',
                    type_=sa.Enum(*old_gender_values, name='gender'),
                    postgresql_using='gender::text::gender')
    op.alter_column('person_history', column_name='gender',
                    type_=sa.Enum(*old_gender_values, name='gender'),
                    postgresql_using='gender::text::gender')
    op.execute('DROP TYPE gender_old;')

    op.execute('ALTER TYPE admission_reason RENAME TO admission_reason_old;')
    sa.Enum(*old_admission_reason_values, name='admission_reason').create(bind=op.get_bind())
    op.alter_column('booking', column_name='admission_reason',
                    type_=sa.Enum(*old_admission_reason_values, name='admission_reason'),
                    postgresql_using='admission_reason::text::admission_reason')
    op.alter_column('booking_history', column_name='admission_reason',
                    type_=sa.Enum(*old_admission_reason_values, name='admission_reason'),
                    postgresql_using='admission_reason::text::admission_reason')
    op.execute('DROP TYPE admission_reason_old;')

    op.execute('ALTER TYPE charge_class RENAME TO charge_class_old;')
    sa.Enum(*old_charge_class_values, name='charge_class').create(bind=op.get_bind())
    op.alter_column('charge', column_name='class',
                    type_=sa.Enum(*old_charge_class_values, name='charge_class'),
                    postgresql_using='class::text::charge_class')
    op.alter_column('charge_history', column_name='class',
                    type_=sa.Enum(*old_charge_class_values, name='charge_class'),
                    postgresql_using='class::text::charge_class')
    op.execute('DROP TYPE charge_class_old;')
