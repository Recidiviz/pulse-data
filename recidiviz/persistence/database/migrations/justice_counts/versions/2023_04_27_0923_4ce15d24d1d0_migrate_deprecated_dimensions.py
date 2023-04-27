# pylint: skip-file
"""migrate deprecated dimensions

Revision ID: 4ce15d24d1d0
Revises: 4862bdccede3
Create Date: 2023-04-27 09:23:34.706512

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4ce15d24d1d0"
down_revision = "4862bdccede3"
branch_labels = None
depends_on = None

# Seattle PD
UPDATE_QUERY_1 = """
update datapoint
set dimension_identifier_to_member='{"global/biological_sex": "FEMALE"}'
where dimension_identifier_to_member='{"global/gender/restricted": "FEMALE"}';
"""
UPDATE_QUERY_2 = """
update datapoint
set dimension_identifier_to_member='{"global/biological_sex": "MALE"}'
where dimension_identifier_to_member='{"global/gender/restricted": "MALE"}';
"""
UPDATE_QUERY_3 = """
update datapoint
set dimension_identifier_to_member='{"global/biological_sex": "UNKNOWN"}'
where dimension_identifier_to_member='{"global/gender/restricted": "NON_BINARY"}';
"""
UPDATE_QUERY_4 = """
update datapoint
set dimension_identifier_to_member='{"global/biological_sex": "UNKNOWN"}'
where dimension_identifier_to_member='{"global/gender/restricted": "OTHER"}';
"""
UPDATE_QUERY_5 = """
update datapoint
set dimension_identifier_to_member='{"global/biological_sex": "UNKNOWN"}'
where dimension_identifier_to_member='{"global/gender/restricted": "UNKNOWN"}';
"""
UPDATE_QUERY_6 = """
update datapoint
set dimension_identifier_to_member='{"global/race_and_ethnicity": "NOT_HISPANIC_AMERICAN_INDIAN_ALASKAN_NATIVE"}'
where dimension_identifier_to_member='{"global/race_and_ethnicity": "AMERICAN_INDIAN_ALASKAN_NATIVE"}';
"""
UPDATE_QUERY_7 = """
update datapoint
set dimension_identifier_to_member='{"global/race_and_ethnicity": "NOT_HISPANIC_ASIAN"}'
where dimension_identifier_to_member='{"global/race_and_ethnicity": "ASIAN"}';
"""
UPDATE_QUERY_8 = """
update datapoint
set dimension_identifier_to_member='{"global/race_and_ethnicity": "NOT_HISPANIC_BLACK"}'
where dimension_identifier_to_member='{"global/race_and_ethnicity": "BLACK"}';
"""
UPDATE_QUERY_9 = """
update datapoint
set dimension_identifier_to_member='{"global/race_and_ethnicity": "NOT_HISPANIC_UNKNOWN"}'
where dimension_identifier_to_member='{"global/race_and_ethnicity": "EXTERNAL_UNKNOWN"}';
"""
UPDATE_QUERY_10 = """
update datapoint
set dimension_identifier_to_member='{"global/race_and_ethnicity": "HISPANIC_UNKNOWN"}'
where dimension_identifier_to_member='{"global/race_and_ethnicity": "HISPANIC"}';
"""
UPDATE_QUERY_11 = """
update datapoint
set dimension_identifier_to_member='{"global/race_and_ethnicity": "NOT_HISPANIC_NATIVE_HAWAIIAN_PACIFIC_ISLANDER"}'
where dimension_identifier_to_member='{"global/race_and_ethnicity": "NATIVE_HAWAIIAN_PACIFIC_ISLANDER"}';
"""
UPDATE_QUERY_12 = """
update datapoint
set dimension_identifier_to_member='{"global/race_and_ethnicity": "NOT_HISPANIC_OTHER"}'
where dimension_identifier_to_member='{"global/race_and_ethnicity": "OTHER"}';
"""
UPDATE_QUERY_13 = """
update datapoint
set dimension_identifier_to_member='{"global/race_and_ethnicity": "NOT_HISPANIC_WHITE"}'
where dimension_identifier_to_member='{"global/race_and_ethnicity": "WHITE"}';
"""
UPDATE_QUERY_14 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "DRUG"}'
where dimension_identifier_to_member='{"metric/law_enforcement/reported_crime/type": "DRUG"}';
"""
UPDATE_QUERY_15 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "OTHER"}'
where dimension_identifier_to_member='{"metric/law_enforcement/reported_crime/type": "OTHER"}';
"""
UPDATE_QUERY_16 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "PERSON"}'
where dimension_identifier_to_member='{"metric/law_enforcement/reported_crime/type": "PERSON"}';
"""
UPDATE_QUERY_17 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "PROPERTY"}'
where dimension_identifier_to_member='{"metric/law_enforcement/reported_crime/type": "PROPERTY"}';
"""
UPDATE_QUERY_18 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "UNKNOWN"}'
where dimension_identifier_to_member='{"metric/law_enforcement/reported_crime/type": "UNKNOWN"}';
"""
UPDATE_QUERY_19 = """
update datapoint
set dimension_identifier_to_member='{"metric/law_enforcement/officer_use_of_force_incidents/type": "OTHER"}'
where dimension_identifier_to_member='{"metric/law_enforcement/officer_use_of_force_incidents/type": "VERBAL"}';
"""
UPDATE_QUERY_20 = """
update datapoint
set dimension_identifier_to_member='{"metric/law_enforcement/officer_use_of_force_incidents/type": "FIREARM"}'
where dimension_identifier_to_member='{"metric/law_enforcement/officer_use_of_force_incidents/type": "WEAPON"}';
"""
UPDATE_QUERY_21 = """
update datapoint
set dimension_identifier_to_member='{"metric/expense/type": "FACILITIES"}'
where dimension_identifier_to_member='{"metric/law_enforcement/expense/type": "FACILITIES_AND_EQUIPMENT"}';
"""
UPDATE_QUERY_22 = """
update datapoint
set dimension_identifier_to_member='{"metric/expense/type": "OTHER"}'
where dimension_identifier_to_member='{"metric/law_enforcement/expense/type": "OTHER"}';
"""
UPDATE_QUERY_23 = """
update datapoint
set dimension_identifier_to_member='{"metric/expense/type": "PERSONNEL"}'
where dimension_identifier_to_member='{"metric/law_enforcement/expense/type": "PERSONNEL"}';
"""
UPDATE_QUERY_24 = """
update datapoint
set dimension_identifier_to_member='{"metric/expense/type": "TRAINING"}'
where dimension_identifier_to_member='{"metric/law_enforcement/expense/type": "TRAINING"}';
"""
UPDATE_QUERY_25 = """
update datapoint
set dimension_identifier_to_member='{"metric/expense/type": "UNKNOWN"}'
where dimension_identifier_to_member='{"metric/law_enforcement/expense/type": "UNKNOWN"}';
"""

# Douglas County
UPDATE_QUERY_26 = """
update datapoint
set dimension_identifier_to_member='{"metric/severity/prosecution_defense/type": "FELONY"}'
where dimension_identifier_to_member='{"metric/severity/prosecution/type": "FELONY"}';
"""
UPDATE_QUERY_27 = """
update datapoint
set dimension_identifier_to_member='{"metric/severity/prosecution_defense/type": "OTHER"}'
where dimension_identifier_to_member='{"metric/severity/prosecution/type": "INFRACTION"}';
"""
UPDATE_QUERY_28 = """
update datapoint
set dimension_identifier_to_member='{"metric/severity/prosecution_defense/type": "MISDEMEANOR"}'
where dimension_identifier_to_member='{"metric/severity/prosecution/type": "MISDEMEANOR"}';
"""
UPDATE_QUERY_29 = """
update datapoint
set dimension_identifier_to_member='{"metric/severity/prosecution_defense/type": "UNKNOWN"}'
where dimension_identifier_to_member='{"metric/severity/prosecution/type": "UNKNOWN"}';
"""

# Washington DOC
UPDATE_QUERY_36 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "DRUG"}'
where dimension_identifier_to_member='{"metric/prisons/offense/type": "DRUG"}';
"""
UPDATE_QUERY_37 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "OTHER"}'
where dimension_identifier_to_member='{"metric/prisons/offense/type": "OTHER"}';
"""
UPDATE_QUERY_38 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "PERSON"}'
where dimension_identifier_to_member='{"metric/prisons/offense/type": "PERSON"}';
"""
UPDATE_QUERY_39 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "PROPERTY"}'
where dimension_identifier_to_member='{"metric/prisons/offense/type": "PROPERTY"}';
"""
UPDATE_QUERY_40 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "PUBLIC_ORDER"}'
where dimension_identifier_to_member='{"metric/prisons/offense/type": "PUBLIC_ORDER"}';
"""
UPDATE_QUERY_41 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "UNKNOWN"}'
where dimension_identifier_to_member='{"metric/prisons/offense/type": "UNKNOWN"}';
"""

# Georiga DCS
UPDATE_QUERY_46 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "DRUG"}'
where dimension_identifier_to_member='{"metric/supervision/offense/type": "DRUG"}';
"""
UPDATE_QUERY_47 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "OTHER"}'
where dimension_identifier_to_member='{"metric/supervision/offense/type": "OTHER"}';
"""
UPDATE_QUERY_48 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "PROPERTY"}'
where dimension_identifier_to_member='{"metric/supervision/offense/type": "PROPERTY"}';
"""
UPDATE_QUERY_49 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "UNKNOWN"}'
where dimension_identifier_to_member='{"metric/supervision/offense/type": "UNKNOWN"}';
"""
UPDATE_QUERY_50 = """
update datapoint
set dimension_identifier_to_member='{"metric/offense/type": "PERSON"}'
where dimension_identifier_to_member='{"metric/supervision/offense/type": "VIOLENT"}';
"""
UPDATE_QUERY_51 = """
update datapoint
set dimension_identifier_to_member='{"metric/staff/supervision/type": "SUPERVISION"}'
where dimension_identifier_to_member='{"metric/staff/supervision/type": "SUPERVISION_OFFICERS"}';
"""
UPDATE_QUERY_52 = """
update datapoint
set dimension_identifier_to_member='{"metric/staff/supervision/type": "MANAGEMENT_AND_OPERATIONS"}'
where dimension_identifier_to_member='{"metric/staff/supervision/type": "SUPPORT"}';
"""


def upgrade() -> None:
    connection = op.get_bind()
    connection.execute(UPDATE_QUERY_1)
    connection.execute(UPDATE_QUERY_2)
    connection.execute(UPDATE_QUERY_3)
    connection.execute(UPDATE_QUERY_4)
    connection.execute(UPDATE_QUERY_5)
    connection.execute(UPDATE_QUERY_6)
    connection.execute(UPDATE_QUERY_7)
    connection.execute(UPDATE_QUERY_8)
    connection.execute(UPDATE_QUERY_9)
    connection.execute(UPDATE_QUERY_10)
    connection.execute(UPDATE_QUERY_11)
    connection.execute(UPDATE_QUERY_12)
    connection.execute(UPDATE_QUERY_13)
    connection.execute(UPDATE_QUERY_14)
    connection.execute(UPDATE_QUERY_15)
    connection.execute(UPDATE_QUERY_16)
    connection.execute(UPDATE_QUERY_17)
    connection.execute(UPDATE_QUERY_18)
    connection.execute(UPDATE_QUERY_19)
    connection.execute(UPDATE_QUERY_20)
    connection.execute(UPDATE_QUERY_21)
    connection.execute(UPDATE_QUERY_22)
    connection.execute(UPDATE_QUERY_23)
    connection.execute(UPDATE_QUERY_24)
    connection.execute(UPDATE_QUERY_25)
    connection.execute(UPDATE_QUERY_26)
    connection.execute(UPDATE_QUERY_27)
    connection.execute(UPDATE_QUERY_28)
    connection.execute(UPDATE_QUERY_29)
    connection.execute(UPDATE_QUERY_36)
    connection.execute(UPDATE_QUERY_37)
    connection.execute(UPDATE_QUERY_38)
    connection.execute(UPDATE_QUERY_39)
    connection.execute(UPDATE_QUERY_40)
    connection.execute(UPDATE_QUERY_41)
    connection.execute(UPDATE_QUERY_46)
    connection.execute(UPDATE_QUERY_47)
    connection.execute(UPDATE_QUERY_48)
    connection.execute(UPDATE_QUERY_49)
    connection.execute(UPDATE_QUERY_50)
    connection.execute(UPDATE_QUERY_51)
    connection.execute(UPDATE_QUERY_52)


def downgrade() -> None:
    return None
