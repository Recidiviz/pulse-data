"""fips_length_constraint

Revision ID: cf6457856a78
Revises: fc4149d309f2
Create Date: 2019-03-22 14:39:14.958496

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'cf6457856a78'
down_revision = 'fc4149d309f2'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('ALTER TABLE ca_facility_aggregate '
               'ADD CONSTRAINT ca_facility_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE fl_county_aggregate '
               'ADD CONSTRAINT fl_county_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE fl_facility_aggregate '
               'ADD CONSTRAINT fl_facility_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE ga_county_aggregate '
               'ADD CONSTRAINT ga_county_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE hi_facility_aggregate '
               'ADD CONSTRAINT hi_facility_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE ky_facility_aggregate '
               'ADD CONSTRAINT ky_facility_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE ny_facility_aggregate '
               'ADD CONSTRAINT ny_facility_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE tx_county_aggregate '
               'ADD CONSTRAINT tx_county_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE dc_facility_aggregate '
               'ADD CONSTRAINT dc_facility_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE pa_facility_pop_aggregate '
               'ADD CONSTRAINT pa_facility_pop_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE pa_county_pre_sentenced_aggregate '
               'ADD CONSTRAINT pa_county_pre_sentenced_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE tn_facility_aggregate '
               'ADD CONSTRAINT tn_facility_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')
    op.execute('ALTER TABLE tn_facility_female_aggregate '
               'ADD CONSTRAINT tn_facility_female_aggregate_fips_length_check '
               'CHECK (LENGTH(fips) = 5);')


def downgrade():
    raise NotImplementedError() 
