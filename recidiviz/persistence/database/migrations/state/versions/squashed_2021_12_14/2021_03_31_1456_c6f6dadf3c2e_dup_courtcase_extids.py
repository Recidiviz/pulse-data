# pylint: skip-file
"""dup_courtcase_extids

Revision ID: c6f6dadf3c2e
Revises: bed035a0cd79
Create Date: 2021-03-31 14:56:50.993590

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "c6f6dadf3c2e"
down_revision = "bed035a0cd79"
branch_labels = None
depends_on = None

DEDUP_UPGRADE_QUERY = """
    WITH id_counts AS (
        SELECT state_code, external_id, person_id,
            COUNT(*) as c,
            ARRAY_AGG({primary_key} ORDER BY {primary_key}) AS {primary_key}s
        FROM {table}
        WHERE external_id IS NOT NULL AND state_code = 'US_ND'
        GROUP BY state_code, external_id, person_id
    ),
    duplicates AS (
        SELECT state_code, external_id, person_id, {primary_key},
        ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY {primary_key}) AS rn
        FROM id_counts, UNNEST({primary_key}s) AS {primary_key}
        WHERE c > 1
    ),
    new_ids AS (
        SELECT state_code, {primary_key}, person_id, external_id AS old_external_id, CONCAT(external_id, '-dup', rn) AS external_id
        FROM duplicates
    )
    UPDATE {table} t
    SET external_id = new_ids.external_id
    FROM new_ids
    WHERE t.{primary_key} = new_ids.{primary_key}
"""


def upgrade() -> None:
    table_to_primary_key = {"state_court_case": "court_case_id"}
    with op.get_context().autocommit_block():
        for table, primary_key in table_to_primary_key.items():
            op.execute(
                StrictStringFormatter().format(
                    DEDUP_UPGRADE_QUERY, table=table, primary_key=primary_key
                )
            )


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
