# pylint: skip-file
"""add_state_code_to_association_tables

Revision ID: 423053bfa215
Revises: 841f9f4944f9
Create Date: 2020-08-21 12:30:47.230993

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '423053bfa215'
down_revision = '841f9f4944f9'
branch_labels = None
depends_on = None

UPDATE_QUERY = \
    "WITH state_code_for_foreign_key AS (SELECT {foreign_key_table}.state_code, " \
    "{foreign_key_table}.{foreign_key_col} FROM {foreign_key_table}, {association_table} " \
    "WHERE {association_table}.{foreign_key_col} = {foreign_key_table}.{foreign_key_col} AND " \
    "{association_table}.state_code IS NULL) " \
    "UPDATE {association_table} a " \
    "SET state_code = state_code_for_foreign_key.state_code " \
    "FROM state_code_for_foreign_key " \
    "WHERE state_code_for_foreign_key.{foreign_key_col} = a.{foreign_key_col} "


def upgrade():
    connection = op.get_bind()

    print("Starting: (1) state_charge_fine_association")
    op.add_column('state_charge_fine_association', sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='charge_id',
                            foreign_key_table='state_charge',
                            association_table='state_charge_fine_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_charge_fine_association', 'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_charge_fine_association_state_code'),
                    'state_charge_fine_association', ['state_code'], unique=False)
    print("Finished: (1) state_charge_fine_association")

    print("Starting: (2) state_charge_incarceration_sentence_association")
    op.add_column('state_charge_incarceration_sentence_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='charge_id',
                            foreign_key_table='state_charge',
                            association_table='state_charge_incarceration_sentence_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_charge_incarceration_sentence_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_charge_incarceration_sentence_association_state_code'),
                    'state_charge_incarceration_sentence_association', ['state_code'], unique=False)
    print("Finished: (2) state_charge_incarceration_sentence_association")

    print("Starting: (3) state_charge_incarceration_sentence_association")
    op.add_column('state_charge_supervision_sentence_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='charge_id',
                            foreign_key_table='state_charge',
                            association_table='state_charge_supervision_sentence_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_charge_supervision_sentence_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_charge_supervision_sentence_association_state_code'),
                    'state_charge_supervision_sentence_association', ['state_code'], unique=False)
    print("Finished: (3) state_charge_incarceration_sentence_association")

    print("Starting: (4) state_incarceration_period_program_assignment_association")
    op.add_column('state_incarceration_period_program_assignment_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='incarceration_period_id',
                            foreign_key_table='state_incarceration_period',
                            association_table='state_incarceration_period_program_assignment_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_incarceration_period_program_assignment_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_incarceration_period_program_assignment_association_state_code'),
                    'state_incarceration_period_program_assignment_association', ['state_code'], unique=False)
    print("Finished: (4) state_incarceration_period_program_assignment_association")

    print("Starting: (5) state_incarceration_sentence_incarceration_period_association")
    op.add_column('state_incarceration_sentence_incarceration_period_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='incarceration_sentence_id',
                            foreign_key_table='state_incarceration_sentence',
                            association_table='state_incarceration_sentence_incarceration_period_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_incarceration_sentence_incarceration_period_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_incarceration_sentence_incarceration_period_association_state_code'),
                    'state_incarceration_sentence_incarceration_period_association', ['state_code'], unique=False)
    print("Finished: (5) state_incarceration_sentence_incarceration_period_association")

    print("Starting: (6) state_incarceration_sentence_supervision_period_association")
    op.add_column('state_incarceration_sentence_supervision_period_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='incarceration_sentence_id',
                            foreign_key_table='state_incarceration_sentence',
                            association_table='state_incarceration_sentence_supervision_period_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_incarceration_sentence_supervision_period_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_incarceration_sentence_supervision_period_association_state_code'),
                    'state_incarceration_sentence_supervision_period_association', ['state_code'], unique=False)
    print("Finished: (6) state_incarceration_sentence_supervision_period_association")

    print("Starting: (7) state_parole_decision_decision_agent_association")
    op.add_column('state_parole_decision_decision_agent_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='parole_decision_id',
                            foreign_key_table='state_parole_decision',
                            association_table='state_parole_decision_decision_agent_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_parole_decision_decision_agent_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_parole_decision_decision_agent_association_state_code'),
                    'state_parole_decision_decision_agent_association', ['state_code'], unique=False)
    print("Finished: (7) state_parole_decision_decision_agent_association")

    print("Starting: (8) state_supervision_period_program_assignment_association")
    op.add_column('state_supervision_period_program_assignment_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='supervision_period_id',
                            foreign_key_table='state_supervision_period',
                            association_table='state_supervision_period_program_assignment_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_supervision_period_program_assignment_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_supervision_period_program_assignment_association_state_code'),
                    'state_supervision_period_program_assignment_association', ['state_code'], unique=False)
    print("Finished: (8) state_supervision_period_program_assignment_association")

    print("Starting: (9) state_supervision_period_supervision_contact_association")
    op.add_column('state_supervision_period_supervision_contact_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='supervision_period_id',
                            foreign_key_table='state_supervision_period',
                            association_table='state_supervision_period_supervision_contact_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_supervision_period_supervision_contact_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_supervision_period_supervision_contact_association_state_code'),
                    'state_supervision_period_supervision_contact_association', ['state_code'], unique=False)
    print("Finished: (9) state_supervision_period_supervision_contact_association")

    print("Starting: (10) state_supervision_period_supervision_violation_association")
    op.add_column('state_supervision_period_supervision_violation_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='supervision_period_id',
                            foreign_key_table='state_supervision_period',
                            association_table='state_supervision_period_supervision_violation_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_supervision_period_supervision_violation_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_supervision_period_supervision_violation_association_state_code'),
                    'state_supervision_period_supervision_violation_association', ['state_code'], unique=False)
    print("Finished: (10) state_supervision_period_supervision_violation_association")

    print("Starting: (11) state_supervision_sentence_incarceration_period_association")
    op.add_column('state_supervision_sentence_incarceration_period_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='supervision_sentence_id',
                            foreign_key_table='state_supervision_sentence',
                            association_table='state_supervision_sentence_incarceration_period_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_supervision_sentence_incarceration_period_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_supervision_sentence_incarceration_period_association_state_code'),
                    'state_supervision_sentence_incarceration_period_association', ['state_code'], unique=False)
    print("Finished: (11) state_supervision_sentence_incarceration_period_association")

    print("Starting: (12) state_supervision_sentence_supervision_period_association")
    op.add_column('state_supervision_sentence_supervision_period_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='supervision_sentence_id',
                            foreign_key_table='state_supervision_sentence',
                            association_table='state_supervision_sentence_supervision_period_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_supervision_sentence_supervision_period_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_supervision_sentence_supervision_period_association_state_code'),
                    'state_supervision_sentence_supervision_period_association', ['state_code'], unique=False)
    print("Finished: (12) state_supervision_sentence_supervision_period_association")

    print("Starting: (13) state_supervision_violation_response_decision_agent_association")
    op.add_column('state_supervision_violation_response_decision_agent_association',
                  sa.Column('state_code', sa.String(length=5), nullable=True))
    association_table_update_query = \
        UPDATE_QUERY.format(foreign_key_col='supervision_violation_response_id',
                            foreign_key_table='state_supervision_violation_response',
                            association_table='state_supervision_violation_response_decision_agent_association')
    connection.execute(association_table_update_query)
    op.alter_column('state_supervision_violation_response_decision_agent_association',
                    'state_code', existing_type=sa.String(), nullable=False)
    op.create_index(op.f('ix_state_supervision_violation_response_decision_agent_association_state_code'),
                    'state_supervision_violation_response_decision_agent_association', ['state_code'], unique=False)
    print("Finished: (13) state_supervision_violation_response_decision_agent_association")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_state_supervision_violation_response_decision_agent_association_state_code'),
                  table_name='state_supervision_violation_response_decision_agent_association')
    op.drop_column('state_supervision_violation_response_decision_agent_association', 'state_code')
    op.drop_index(op.f('ix_state_supervision_sentence_supervision_period_association_state_code'),
                  table_name='state_supervision_sentence_supervision_period_association')
    op.drop_column('state_supervision_sentence_supervision_period_association', 'state_code')
    op.drop_index(op.f('ix_state_supervision_sentence_incarceration_period_association_state_code'),
                  table_name='state_supervision_sentence_incarceration_period_association')
    op.drop_column('state_supervision_sentence_incarceration_period_association', 'state_code')
    op.drop_index(op.f('ix_state_supervision_period_supervision_violation_association_state_code'),
                  table_name='state_supervision_period_supervision_violation_association')
    op.drop_column('state_supervision_period_supervision_violation_association', 'state_code')
    op.drop_index(op.f('ix_state_supervision_period_supervision_contact_association_state_code'),
                  table_name='state_supervision_period_supervision_contact_association')
    op.drop_column('state_supervision_period_supervision_contact_association', 'state_code')
    op.drop_index(op.f('ix_state_supervision_period_program_assignment_association_state_code'),
                  table_name='state_supervision_period_program_assignment_association')
    op.drop_column('state_supervision_period_program_assignment_association', 'state_code')
    op.drop_index(op.f('ix_state_parole_decision_decision_agent_association_state_code'),
                  table_name='state_parole_decision_decision_agent_association')
    op.drop_column('state_parole_decision_decision_agent_association', 'state_code')
    op.drop_index(op.f('ix_state_incarceration_sentence_supervision_period_association_state_code'),
                  table_name='state_incarceration_sentence_supervision_period_association')
    op.drop_column('state_incarceration_sentence_supervision_period_association', 'state_code')
    op.drop_index(op.f('ix_state_incarceration_sentence_incarceration_period_association_state_code'),
                  table_name='state_incarceration_sentence_incarceration_period_association')
    op.drop_column('state_incarceration_sentence_incarceration_period_association', 'state_code')
    op.drop_index(op.f('ix_state_incarceration_period_program_assignment_association_state_code'),
                  table_name='state_incarceration_period_program_assignment_association')
    op.drop_column('state_incarceration_period_program_assignment_association', 'state_code')
    op.drop_index(op.f('ix_state_charge_supervision_sentence_association_state_code'),
                  table_name='state_charge_supervision_sentence_association')
    op.drop_column('state_charge_supervision_sentence_association', 'state_code')
    op.drop_index(op.f('ix_state_charge_incarceration_sentence_association_state_code'),
                  table_name='state_charge_incarceration_sentence_association')
    op.drop_column('state_charge_incarceration_sentence_association', 'state_code')
    op.drop_index(op.f('ix_state_charge_fine_association_state_code'),
                  table_name='state_charge_fine_association')
    op.drop_column('state_charge_fine_association', 'state_code')
    # ### end Alembic commands ###
