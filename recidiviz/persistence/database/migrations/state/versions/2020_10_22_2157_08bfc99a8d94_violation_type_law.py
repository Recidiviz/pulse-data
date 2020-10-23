# pylint: skip-file
"""violation_type_law

Revision ID: 08bfc99a8d94
Revises: c578e84c3752
Create Date: 2020-10-22 21:57:23.816221

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '08bfc99a8d94'
down_revision = 'c578e84c3752'
branch_labels = None
depends_on = None


# Without new value
old_values = ['ABSCONDED', 'ESCAPED', 'FELONY', 'MISDEMEANOR', 'MUNICIPAL', 'TECHNICAL']

# With new value
new_values = ['ABSCONDED', 'ESCAPED', 'FELONY', 'LAW', 'MISDEMEANOR', 'MUNICIPAL', 'TECHNICAL']


SELECT_VIOLATION_TYPE_IDS_QUERY = \
    "SELECT {violation_type_table_id_field} FROM" \
    " {violation_type_table}" \
    " WHERE state_code = '{state_code}' AND violation_type_raw_text = '{violation_type_raw_text_old_value}'"


UPDATE_QUERY = f"UPDATE {{violation_type_table}} " \
               f"SET violation_type = '{{violation_type_new_value}}', " \
               f"  violation_type_raw_text = '{{violation_type_raw_text_new_value}}'" \
               f" WHERE {{violation_type_table_id_field}} IN ({SELECT_VIOLATION_TYPE_IDS_QUERY});"


TABLES_TO_UPDATE_WITH_ID_FIELDS = \
    [('state_supervision_violation', 'supervision_violation_id'),
     ('state_supervision_violation_history', 'supervision_violation_history_id'),
     ('state_supervision_violation_type_entry', 'supervision_violation_type_entry_id'),
     ('state_supervision_violation_type_entry_history', 'supervision_violation_type_history_id')]

def upgrade() -> None:
    # Add new enum value
    op.execute('ALTER TYPE state_supervision_violation_type RENAME TO state_supervision_violation_type_old;')
    sa.Enum(*new_values, name='state_supervision_violation_type').create(bind=op.get_bind())
    op.alter_column('state_supervision_violation', column_name='violation_type',
                    type_=sa.Enum(*new_values, name='state_supervision_violation_type'),
                    postgresql_using='violation_type::text::state_supervision_violation_type')
    op.alter_column('state_supervision_violation_history', column_name='violation_type',
                    type_=sa.Enum(*new_values, name='state_supervision_violation_type'),
                    postgresql_using='violation_type::text::state_supervision_violation_type')

    op.alter_column('state_supervision_violation_type_entry', column_name='violation_type',
                    type_=sa.Enum(*new_values, name='state_supervision_violation_type'),
                    postgresql_using='violation_type::text::state_supervision_violation_type')
    op.alter_column('state_supervision_violation_type_entry_history', column_name='violation_type',
                    type_=sa.Enum(*new_values, name='state_supervision_violation_type'),
                    postgresql_using='violation_type::text::state_supervision_violation_type')

    op.execute('DROP TYPE state_supervision_violation_type_old;')

    # Migrate some state objects to use new value
    connection = op.get_bind()
    for table_name, table_id_field in TABLES_TO_UPDATE_WITH_ID_FIELDS:
        ND_UPGRADE_QUERY = UPDATE_QUERY.format(
            state_code='US_ND',
            violation_type_table=table_name,
            violation_type_table_id_field=table_id_field,
            violation_type_raw_text_old_value='FELONY',
            violation_type_raw_text_new_value='LAW',
            violation_type_new_value='LAW'
        )

        PA_UPGRADE_QUERY_SUMMARY_OFFENSE = UPDATE_QUERY.format(
            state_code='US_PA',
            violation_type_table=table_name,
            violation_type_table_id_field=table_id_field,
            violation_type_raw_text_old_value='M13',  # Conviction of a summary offense
            violation_type_raw_text_new_value='M13',
            violation_type_new_value='LAW'
        )

        PA_UPGRADE_QUERY_MISDEMEANOR = UPDATE_QUERY.format(
            state_code='US_PA',
            violation_type_table=table_name,
            violation_type_table_id_field=table_id_field,
            violation_type_raw_text_old_value='M20',  # Conviction of Misdemeanor Offense
            violation_type_raw_text_new_value='M20',
            violation_type_new_value='LAW'
        )

        PA_UPGRADE_QUERY_PENDING_CHARGE = UPDATE_QUERY.format(
            state_code='US_PA',
            violation_type_table=table_name,
            violation_type_table_id_field=table_id_field,
            violation_type_raw_text_old_value='H04',  # Pending criminal charges (UCV) Detained/Not detained
            violation_type_raw_text_new_value='H04',
            violation_type_new_value='LAW'
        )

        connection.execute(ND_UPGRADE_QUERY)
        connection.execute(PA_UPGRADE_QUERY_SUMMARY_OFFENSE)
        connection.execute(PA_UPGRADE_QUERY_MISDEMEANOR)
        connection.execute(PA_UPGRADE_QUERY_PENDING_CHARGE)


def downgrade() -> None:
    # Migrate some state objects back to old values
    connection = op.get_bind()
    for table_name, table_id_field in TABLES_TO_UPDATE_WITH_ID_FIELDS:
        ND_DOWNGRADE_QUERY = UPDATE_QUERY.format(
            state_code='US_ND',
            violation_type_table=table_name,
            violation_type_table_id_field=table_id_field,
            violation_type_raw_text_old_value='LAW',
            violation_type_raw_text_new_value='FELONY',
            violation_type_new_value='FELONY'
        )

        PA_DOWNGRADE_QUERY_SUMMARY_OFFENSE = UPDATE_QUERY.format(
            state_code='US_PA',
            violation_type_table=table_name,
            violation_type_table_id_field=table_id_field,
            violation_type_raw_text_old_value='M13',  # Conviction of a summary offense
            violation_type_raw_text_new_value='M13',
            violation_type_new_value='MUNICIPAL'
        )

        PA_DOWNGRADE_QUERY_MISDEMEANOR = UPDATE_QUERY.format(
            state_code='US_PA',
            violation_type_table=table_name,
            violation_type_table_id_field=table_id_field,
            violation_type_raw_text_old_value='M20',  # Conviction of Misdemeanor Offense
            violation_type_raw_text_new_value='M20',
            violation_type_new_value='MISDEMEANOR'
        )

        PA_DOWNGRADE_QUERY_PENDING_CHARGE = UPDATE_QUERY.format(
            state_code='US_PA',
            violation_type_table=table_name,
            violation_type_table_id_field=table_id_field,
            violation_type_raw_text_old_value='H04',  # Pending criminal charges (UCV) Detained/Not detained
            violation_type_raw_text_new_value='H04',
            violation_type_new_value='FELONY'
        )

        connection.execute(ND_DOWNGRADE_QUERY)
        connection.execute(PA_DOWNGRADE_QUERY_SUMMARY_OFFENSE)
        connection.execute(PA_DOWNGRADE_QUERY_MISDEMEANOR)
        connection.execute(PA_DOWNGRADE_QUERY_PENDING_CHARGE)

    # Remove new enum value
    op.execute('ALTER TYPE state_supervision_violation_type RENAME TO state_supervision_violation_type_old;')
    sa.Enum(*old_values, name='state_supervision_violation_type').create(bind=op.get_bind())
    op.alter_column('state_supervision_violation', column_name='violation_type',
                    type_=sa.Enum(*old_values, name='state_supervision_violation_type'),
                    postgresql_using='violation_type::text::state_supervision_violation_type')
    op.alter_column('state_supervision_violation_history', column_name='violation_type',
                    type_=sa.Enum(*old_values, name='state_supervision_violation_type'),
                    postgresql_using='violation_type::text::state_supervision_violation_type')

    op.alter_column('state_supervision_violation_type_entry', column_name='violation_type',
                    type_=sa.Enum(*old_values, name='state_supervision_violation_type'),
                    postgresql_using='violation_type::text::state_supervision_violation_type')
    op.alter_column('state_supervision_violation_type_entry_history', column_name='violation_type',
                    type_=sa.Enum(*old_values, name='state_supervision_violation_type'),
                    postgresql_using='violation_type::text::state_supervision_violation_type')

    op.execute('DROP TYPE state_supervision_violation_type_old;')
