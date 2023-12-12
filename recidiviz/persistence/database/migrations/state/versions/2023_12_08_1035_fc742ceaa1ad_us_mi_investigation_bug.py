# pylint: skip-file
"""us_mi_investigation_bug

Revision ID: fc742ceaa1ad
Revises: 09f505a2a4c1
Create Date: 2023-12-08 10:35:32.212197

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fc742ceaa1ad"
down_revision = "09f505a2a4c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ----------------------------------------
        -- Migrate supervision levels from COMS
        ----------------------------------------

        UPDATE state_supervision_period
        SET supervision_type = 'PAROLE', supervision_type_raw_text = supervision_level_raw_text
        WHERE
            state_code = 'US_MI'
            and supervision_type_raw_text = 'INVESTIGATION-MOVEMENT_REASON_ID-2'
            and supervision_level_raw_text like '%PAROLE%';

        UPDATE state_supervision_period
        SET supervision_type = 'PROBATION', supervision_type_raw_text = supervision_level_raw_text
        WHERE
            state_code = 'US_MI'
            and supervision_type_raw_text = 'INVESTIGATION-MOVEMENT_REASON_ID-2'
            and supervision_level_raw_text like '%PROBATION%';

        UPDATE state_supervision_period
        SET supervision_type = 'DUAL', supervision_type_raw_text = supervision_level_raw_text
        WHERE
            state_code = 'US_MI'
            and supervision_type_raw_text = 'INVESTIGATION-MOVEMENT_REASON_ID-2'
            and supervision_level_raw_text like '%PAROLE%'
            and supervision_level_raw_text like '%PROBATION%';

        UPDATE state_supervision_period
        SET supervision_type = 'INTERNAL_UNKNOWN', supervision_type_raw_text = NULL
        WHERE
            state_code = 'US_MI'
            and supervision_type_raw_text = 'INVESTIGATION-MOVEMENT_REASON_ID-2'
            and supervision_level_raw_text not like '%PAROLE%'
            and supervision_level_raw_text not like '%PROBATION%';

        ------------------------------------------------------------------------------------------------------------------------
        -- Migrate supervision levels from OMNI (only includes levels that actually show up in the data in this scenario)
        ------------------------------------------------------------------------------------------------------------------------

        UPDATE state_supervision_period
        SET supervision_type = 'PAROLE', supervision_type_raw_text = CONCAT('LEGAL_ORDER_NULL-SUPERVISION_LEVEL_ID-', supervision_level_raw_text)
        WHERE
            state_code = 'US_MI'
            and supervision_type_raw_text = 'INVESTIGATION-MOVEMENT_REASON_ID-2'
            and supervision_level_raw_text in (
                    '14197', '13597', '14193', '13577', '13574', '13569', '14052', '2281', 
                    '13561', '14218', '2294', '13580', '13573', '14196', '13564', '14049', 
                    '14239', '2292', '13584', '7500', '13825', '13821', '13566', '13832', 
                    '4058', '14050', '8172', '14187', '2282', '13829', '13576', '14051', 
                    '2293', '7183', '13582', '13567', '14181', '2283', '7405', '13828', 
                    '13623', '3636', '13583', '13581', '14221', '2280', '14189', '13572', 
                    '14204', '13579', '13826', '15901', '3810', '13560', '13578', '13575', 
                    '13830', '13827', '13822'
            );

        UPDATE state_supervision_period
        SET supervision_type = 'PROBATION', supervision_type_raw_text = CONCAT('LEGAL_ORDER_NULL-SUPERVISION_LEVEL_ID-', supervision_level_raw_text)
        WHERE
            state_code = 'US_MI'
            and supervision_type_raw_text = 'INVESTIGATION-MOVEMENT_REASON_ID-2'
            and supervision_level_raw_text in (
                    '3799', '3796', '13622', '14164', '14223', '14239', '14195', '14172', 
                    '3629', '3632', '7502', '14237', '7503', '13820', '14222', '13563', 
                    '7507', '3627', '3624', '2289', '14165', '4055', '3626', '3625', '13562', 
                    '13824', '7504', '4053', '14236', '2285', '14198', '3631', '3628', '7184', 
                    '19615', '13819', '3795', '3797', '7505', '2396', '13621', '19614', '3802', 
                    '14199', '5769', '3798', '7506', '3630', '2395', '14194', '4054'
            );

        UPDATE state_supervision_period
        SET supervision_type = 'DUAL', supervision_type_raw_text = CONCAT('LEGAL_ORDER_NULL-SUPERVISION_LEVEL_ID-', supervision_level_raw_text)
        WHERE
            state_code = 'US_MI'
            and supervision_type_raw_text = 'INVESTIGATION-MOVEMENT_REASON_ID-2'
            and supervision_level_raw_text in ('14239');

            
        UPDATE state_supervision_period
        SET supervision_type = 'INTERNAL_UNKNOWN', supervision_type_raw_text = NULL
        WHERE
            state_code = 'US_MI'
            and supervision_type_raw_text = 'INVESTIGATION-MOVEMENT_REASON_ID-2'
            and supervision_level_raw_text in (
                '7481', '7318', '7314', '7477', '7316', '7313', '7317', '7225', '7478', 
                '7479', '7172', '2284', '7480', '2287', '7315'
            );

        """
    )


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
