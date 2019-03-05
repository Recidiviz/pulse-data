"""add_granularity_quarterly

Revision ID: f117d53a093c
Revises: c82f37920d63
Create Date: 2019-03-05 17:37:38.029175

"""


from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f117d53a093c'
down_revision = 'c82f37920d63'
branch_labels = None
depends_on = None


# Without new value
old_values = ['DAILY', 'WEEKLY', 'MONTHLY', 'YEARLY']

# With new value
new_values = ['DAILY', 'WEEKLY', 'MONTHLY', 'QUARTERLY', 'YEARLY']


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
    op.alter_column('ky_facility_aggregate', column_name='report_granularity',
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
    op.alter_column('ky_facility_aggregate', column_name='report_granularity',
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
