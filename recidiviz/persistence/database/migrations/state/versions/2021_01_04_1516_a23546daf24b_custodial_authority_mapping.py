# pylint: skip-file
"""custodial_authority_mapping

Revision ID: a23546daf24b
Revises: dd6deec65b5b
Create Date: 2020-01-04 15:16:54.185111

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "a23546daf24b"
down_revision = "dd6deec65b5b"
branch_labels = None
depends_on = None

UPDATE_QUERY_US_ID = """UPDATE {table_id} SET custodial_authority = 
                        (CASE WHEN custodial_authority_raw_text IN ('IS', 'PC') THEN 'OTHER_STATE'
                             WHEN custodial_authority_raw_text = 'DEPORTED' THEN 'OTHER_COUNTRY'
                             WHEN custodial_authority_raw_text = 'FED' THEN 'FEDERAL'
                             WHEN custodial_authority_raw_text IS NOT NULL THEN 'SUPERVISION_AUTHORITY'
                        END)
                        WHERE state_code = 'US_ID';"""

UPDATE_QUERY_US_PA = """UPDATE {table_id} SET custodial_authority =
                            (CASE WHEN supervision_period_supervision_type_raw_text IS NOT NULL THEN
                                    CASE WHEN custodial_authority IN  ('US_PA_COURTS', 'US_PA_PBPP', 'OUT_OF_STATE') THEN 'SUPERVISION_AUTHORITY'
                                         WHEN custodial_authority = 'US_PA_DOC' THEN 'STATE_PRISON'
                                    END
                                ELSE NULL
                            END)
                      WHERE state_code = 'US_PA';"""

DOWNGRADE_QUERY_US_ID = """UPDATE {table_id} SET custodial_authority =
                        (CASE WHEN custodial_authority_raw_text IN ('IS', 'PC') THEN SPLIT_PART(supervision_site, '|', 2)
                             WHEN custodial_authority_raw_text = 'DEPORTED' THEN
                                CASE WHEN SPLIT_PART(supervision_site, '|', 2) = 'DEPORTED' THEN 'DEPORTED'
                                ELSE 'OTHER_COUNTRY' END
                             WHEN custodial_authority_raw_text = 'FED' THEN
                                CASE WHEN supervision_site LIKE '%%FEDERAL%%' OR supervision_site LIKE '%%U.S.%%' THEN SPLIT_PART(supervision_site, '|', 2)
                                ELSE 'FEDERAL'
                                END
                             WHEN custodial_authority_raw_text IS NOT NULL THEN 'US_ID_DOC'
                        END)
                        WHERE state_code = 'US_ID' AND (custodial_authority IS NOT NULL OR supervision_site IS NOT NULL);"""

DOWNGRADE_QUERY_US_PA = """UPDATE {table_id} SET custodial_authority =
                                        (CASE WHEN custodial_authority = 'SUPERVISION_AUTHORITY' THEN
                                            CASE WHEN custodial_authority_raw_text in ('4A', '4B', '4C', '04', '05') THEN 'US_PA_COURTS'
                                                 WHEN custodial_authority_raw_text in ('06', '07', '08') THEN 'OUT_OF_STATE'
                                            ELSE 'US_PA_PBPP' END
                                        WHEN custodial_authority = 'STATE_PRISON' THEN 'US_PA_DOC'
                                        WHEN custodial_authority_raw_text IS NULL AND external_id IS NOT NULL THEN 'US_PA_PBPP' END)
                      WHERE state_code = 'US_PA' AND custodial_authority_raw_text IS NOT NULL;"""

TABLES_TO_UPDATE = [
    "state_incarceration_period_history",
    "state_incarceration_period",
    "state_supervision_period_history",
    "state_supervision_period",
]

CUSTODIAL_AUTHORITY_VALUES = [
    "FEDERAL",
    "OTHER_COUNTRY",
    "OTHER_STATE",
    "SUPERVISION_AUTHORITY",
    "STATE_PRISON",
]
ENUM_TYPE = sa.Enum(*CUSTODIAL_AUTHORITY_VALUES, name="state_custodial_authority")


def upgrade() -> None:
    sa.Enum(*CUSTODIAL_AUTHORITY_VALUES, name="state_custodial_authority").create(
        bind=op.get_bind()
    )

    connection = op.get_bind()

    # Set custodial_authority values to valid enum values
    for table_id in TABLES_TO_UPDATE:
        # Set custodial_authority values for the supervision tables
        if "supervision" in table_id:
            # Set custodial_authority value for US_ID
            connection.execute(
                StrictStringFormatter().format(UPDATE_QUERY_US_ID, table_id=table_id)
            )

            # Set custodial_authority value for US_PA
            connection.execute(
                StrictStringFormatter().format(UPDATE_QUERY_US_PA, table_id=table_id)
            )

        # Convert the custodial_authority column to an enum
        op.alter_column(
            table_id,
            "custodial_authority",
            existing_type=sa.VARCHAR(length=255),
            type_=ENUM_TYPE,
            existing_nullable=True,
            postgresql_using="custodial_authority::state_custodial_authority",
        )


def downgrade() -> None:
    connection = op.get_bind()

    for table_id in TABLES_TO_UPDATE:
        # Convert the custodial_authority column back to a string
        op.alter_column(
            table_id,
            "custodial_authority",
            existing_type=ENUM_TYPE,
            type_=sa.VARCHAR(length=255),
            existing_nullable=True,
        )

        # Set custodial_authority values for the supervision tables
        if "supervision" in table_id:
            # Set original custodial_authority value for US_ID
            connection.execute(
                StrictStringFormatter().format(DOWNGRADE_QUERY_US_ID, table_id=table_id)
            )

            # Set original custodial_authority value for US_PA
            connection.execute(
                StrictStringFormatter().format(DOWNGRADE_QUERY_US_PA, table_id=table_id)
            )

    op.execute("DROP TYPE state_custodial_authority;")
