# pylint: skip-file
"""populate_condition_from_raw_text

Revision ID: e337bbdbe043
Revises: 80c6f79b69ab
Create Date: 2022-12-06 13:51:29.239871

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e337bbdbe043"
down_revision = "80c6f79b69ab"
branch_labels = None
depends_on = None

PA_UPDATE_QUERY = """
    UPDATE state_supervision_violated_condition_entry
    SET condition = ( CASE
                        WHEN condition_raw_text = 'M07'
                            THEN 'EMPLOYMENT'::state_supervision_violated_condition_type
                        WHEN condition_raw_text IN ('H01', 'M04', 'L07', 'M01')
                            THEN 'FAILURE_TO_NOTIFY'::state_supervision_violated_condition_type  
                        WHEN condition_raw_text IN ('H06', 'M02', 'M19')
                            THEN 'FAILURE_TO_REPORT'::state_supervision_violated_condition_type 
                        WHEN condition_raw_text IN ('L03', 'L04', 'L05', 'L06')
                            THEN 'FINANCIAL'::state_supervision_violated_condition_type        
                        WHEN condition_raw_text IN ('H04', 'M13', 'M20', 'H10', 'H11', 'H08')
                            THEN 'LAW'::state_supervision_violated_condition_type 
                        WHEN condition_raw_text IN ('H02', 'M09', 'M10', 'M15', 'H05', 'L01', 'M05', 'M11', 'M12', 'M16')
                            THEN 'SPECIAL_CONDITIONS'::state_supervision_violated_condition_type
                        WHEN condition_raw_text IN ('H03', 'H12', 'L02', 'L08', 'M03', 'M14', 'M17')
                            THEN 'SUBSTANCE'::state_supervision_violated_condition_type   
                        WHEN condition_raw_text IN ('H07', 'M06', 'M08', 'M18')
                            THEN 'TREATMENT_COMPLIANCE'::state_supervision_violated_condition_type                                                                                                                                                                          
                        END)
    WHERE state_code = 'US_PA'   
"""


MO_UPDATE_QUERY = """
    UPDATE state_supervision_violated_condition_entry
    SET condition = ( CASE
                        WHEN condition_raw_text = 'EMP'
                            THEN 'EMPLOYMENT'::state_supervision_violated_condition_type
                        WHEN condition_raw_text IN ('TRA', 'RES')
                            THEN 'FAILURE_TO_NOTIFY'::state_supervision_violated_condition_type
                        WHEN condition_raw_text = 'DIR'
                            THEN 'FAILURE_TO_REPORT'::state_supervision_violated_condition_type
                        WHEN condition_raw_text = 'SPC'
                            THEN 'FINANCIAL'::state_supervision_violated_condition_type
                        WHEN condition_raw_text IN ('LAW', 'WEA')
                            THEN 'LAW'::state_supervision_violated_condition_type
                        WHEN condition_raw_text IN ('ASC', 'SUP', 'INT')
                            THEN 'SPECIAL_CONDITIONS'::state_supervision_violated_condition_type
                        WHEN condition_raw_text = 'DRG'
                            THEN 'SUBSTANCE'::state_supervision_violated_condition_type
                        END)
    WHERE state_code = 'US_MO'   
"""

DOWNGRADE_QUERY = """
    UPDATE state_supervision_violated_condition_entry
    SET condition = 'INTERNAL_UNKNOWN'
    WHERE state_code = 'US_MO' OR state_code = 'US_PA'
"""


def upgrade() -> None:
    op.execute(PA_UPDATE_QUERY)
    op.execute(MO_UPDATE_QUERY)


def downgrade() -> None:
    op.execute(DOWNGRADE_QUERY)
