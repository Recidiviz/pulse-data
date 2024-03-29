# pylint: skip-file
"""dedup_nd_sids

Revision ID: 43640a2cfdf4
Revises: f79f447d3600
Create Date: 2023-07-20 12:41:45.958850

"""
from typing import List, Tuple

import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "43640a2cfdf4"
down_revision = "f79f447d3600"
branch_labels = None
depends_on = None

_ID_VALUE_MAP: List[Tuple[str, str]] = [
    ("'106677'", "'US_ND_SID'"),
    ("'SD388000A0'", "'US_ND_SID'"),
    ("'FL05541044'", "'US_ND_SID'"),
    ("'206348'", "'US_ND_SID'"),
    ("'256957'", "'US_ND_SID'"),
    ("'262006'", "'US_ND_SID'"),
    ("'255989'", "'US_ND_SID'"),
    ("'241724'", "'US_ND_SID'"),
    ("'245149 |'", "'US_ND_SID'"),
    ("'243241'", "'US_ND_SID'"),
    ("'04/19/1981'", "'US_ND_SID'"),
    ("'23677'", "'US_ND_SID'"),
    ("'945541'", "'US_ND_SID'"),
    ("'270092'", "'US_ND_SID'"),
    ("'69798'", "'US_ND_ELITE'"),
    ("'2729601'", "'US_ND_SID'"),
    ("'266431'", "'US_ND_SID'"),
    ("'26339'", "'US_ND_SID'"),
]

UPGRADE_QUERY_TEMPLATE = """
    DELETE FROM state_person_external_id
    WHERE id_type = {id_type}
    AND external_id = {bad_value}
"""


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    for bad_value, id_type in _ID_VALUE_MAP:
        op.execute(
            StrictStringFormatter().format(
                UPGRADE_QUERY_TEMPLATE,
                bad_value=bad_value,
                id_type=id_type,
            )
        )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
