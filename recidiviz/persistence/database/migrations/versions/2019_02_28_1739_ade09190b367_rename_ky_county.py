"""rename_ky_county

Revision ID: ade09190b367
Revises: f359119f069c
Create Date: 2019-02-28 17:39:18.670672

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ade09190b367'
down_revision = 'f359119f069c'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('ALTER TABLE ky_county_aggregate RENAME CONSTRAINT ky_county_aggregate_fips_facility_name_report_date_report_g_key TO ky_facility_aggregate_fips_facility_name_report_date_report_g_key;')
    op.execute('ALTER TABLE ky_county_aggregate RENAME TO ky_facility_aggregate;')
    op.execute('ALTER INDEX ky_county_aggregate_pkey RENAME TO ky_facility_aggregate_pkey;')
    op.execute('ALTER SEQUENCE ky_county_aggregate_record_id_seq RENAME TO ky_facility_aggregate_record_id_seq;')


def downgrade():
    op.execute('ALTER TABLE ky_facility_aggregate RENAME CONSTRAINT ky_facility_aggregate_fips_facility_name_report_date_report_g_key TO ky_county_aggregate_fips_facility_name_report_date_report_g_key;')
    op.execute('ALTER TABLE ky_facility_aggregate RENAME TO ky_county_aggregate;')
    op.execute('ALTER INDEX ky_facility_aggregate_pkey RENAME TO ky_county_aggregate_pkey;')
    op.execute('ALTER SEQUENCE ky_facility_aggregate_record_id_seq RENAME TO ky_county_aggregate_record_id_seq;')
