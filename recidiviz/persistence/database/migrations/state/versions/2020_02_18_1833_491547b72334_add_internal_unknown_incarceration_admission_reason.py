"""add_internal_unknown_incarceration_admission_reason

Revision ID: 491547b72334
Revises: 1b4d14ad4a70
Create Date: 2020-02-18 18:33:31.502400

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '491547b72334'
down_revision = '1b4d14ad4a70'
branch_labels = None
depends_on = None

# Without new value
old_values = ['ADMITTED_IN_ERROR', 'DUAL_REVOCATION', 'EXTERNAL_UNKNOWN', 'NEW_ADMISSION', 'PAROLE_REVOCATION',
              'PROBATION_REVOCATION', 'RETURN_FROM_ERRONEOUS_RELEASE', 'RETURN_FROM_ESCAPE', 'TEMPORARY_CUSTODY',
              'TRANSFER']


# With new value
new_values = ['ADMITTED_IN_ERROR', 'DUAL_REVOCATION', 'EXTERNAL_UNKNOWN', 'INTERNAL_UNKNOWN', 'NEW_ADMISSION',
              'PAROLE_REVOCATION', 'PROBATION_REVOCATION', 'RETURN_FROM_ERRONEOUS_RELEASE', 'RETURN_FROM_ESCAPE',
              'TEMPORARY_CUSTODY', 'TRANSFER']


def upgrade():
    op.execute('ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;')
    sa.Enum(*new_values, name='state_incarceration_period_admission_reason').create(bind=op.get_bind())
    op.alter_column('state_incarceration_period', column_name='admission_reason',
                    type_=sa.Enum(*new_values, name='state_incarceration_period_admission_reason'),
                    postgresql_using='admission_reason::text::state_incarceration_period_admission_reason')
    op.alter_column('state_incarceration_period_history', column_name='admission_reason',
                    type_=sa.Enum(*new_values, name='state_incarceration_period_admission_reason'),
                    postgresql_using='admission_reason::text::state_incarceration_period_admission_reason')
    op.execute('DROP TYPE state_incarceration_period_admission_reason_old;')


def downgrade():
    op.execute('ALTER TYPE state_incarceration_period_admission_reason RENAME TO state_incarceration_period_admission_reason_old;')
    sa.Enum(*old_values, name='state_incarceration_period_admission_reason').create(bind=op.get_bind())
    op.alter_column('state_incarceration_period', column_name='admission_reason',
                    type_=sa.Enum(*old_values, name='state_incarceration_period_admission_reason'),
                    postgresql_using='admission_reason::text::state_incarceration_period_admission_reason')
    op.alter_column('state_incarceration_period_history', column_name='admission_reason',
                    type_=sa.Enum(*old_values, name='state_incarceration_period_admission_reason'),
                    postgresql_using='admission_reason::text::state_incarceration_period_admission_reason')
    op.execute('DROP TYPE state_incarceration_period_admission_reason_old;')
