"""add_report_granularity_yearly

Revision ID: f359119f069c
Revises: 001002570c42
Create Date: 2019-02-28 17:03:21.795093

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f359119f069c'
down_revision = '001002570c42'
branch_labels = None
depends_on = None

# Without new value
old_values = ['DAILY', 'WEEKLY', 'MONTHLY']

# With new value
new_values = ['DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY']

def upgrade():
    op.execute('ALTER TYPE report_granularity RENAME TO report_granularity_old;')
    sa.Enum(*new_values, name='report_granularity').create(bind=op.get_bind())
    op.alter_column('ca_facility_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*new_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('fl_county_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*new_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('fl_facility_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*new_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('ga_county_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*new_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('hi_facility_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*new_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('ky_county_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*new_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('ny_facility_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*new_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('tx_county_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*new_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('dc_facility_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*new_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('pa_facility_pop_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*new_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.execute('DROP TYPE report_granularity_old;')


def downgrade():
    op.execute('ALTER TYPE report_granularity RENAME TO report_granularity_old;')
    sa.Enum(*old_values, name='report_granularity').create(bind=op.get_bind())
    op.alter_column('ca_facility_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*old_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('fl_county_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*old_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('fl_facility_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*old_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('ga_county_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*old_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('hi_facility_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*old_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('ky_county_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*old_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('ny_facility_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*old_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('tx_county_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*old_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('dc_facility_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*old_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.alter_column('pa_facility_pop_aggregate', column_name='report_granularity',
                    type_=sa.Enum(*old_values, name='report_granularity'),
                    postgresql_using='report_granularity::text::report_granularity')
    op.execute('DROP TYPE report_granularity_old;')
