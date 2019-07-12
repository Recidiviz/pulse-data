"""add_booking_ids_to_sentence

Revision ID: 91466ffebeb0
Revises: 23d1a98df3fe
Create Date: 2019-02-21 14:05:20.688644

"""

# Hackity hack to get around the fact that alembic runs this file as a
# top-level module rather than a child of the recidiviz module
import sys
import os
module_path = os.path.abspath(__file__)
# Walk up directories to reach main package
while not module_path.split('/')[-1] == 'recidiviz':
    if module_path == '/':
        raise RuntimeError("Top-level recidiviz package not found")
    module_path = os.path.dirname(module_path)
# Must insert parent directory of main package
sys.path.insert(0, os.path.dirname(module_path))

from alembic import op
import sqlalchemy as sa

from recidiviz.persistence.database.base_schema import JailsBase

# revision identifiers, used by Alembic.
revision = '91466ffebeb0'
down_revision = '23d1a98df3fe'
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()

    # Populate with dummy default value to avoid integrity error around
    # non-null constraint (will be overwritten after)
    op.add_column(
        'sentence',
        sa.Column(
            'booking_id', sa.Integer(), nullable=False, server_default='0'))
    sentence_table_view = sa.Table(
        'sentence',
        JailsBase.metadata,
        sa.Column('sentence_id', sa.Integer(), primary_key=True),
        sa.Column('booking_id', sa.Integer()),
        extend_existing=True)
    for sentence in connection.execute(sentence_table_view.select()):
        # Get any charge history that links to this sentence, since every
        # charge history snapshot will have the same booking ID. Use charge
        # history instead of charge since even sentences that are orphaned on
        # master will still be pointed to from charge history.
        booking_id = connection.execute(
            'SELECT charge_history.booking_id FROM charge_history WHERE '
            'charge_history.sentence_id = {} LIMIT 1'.format(
                sentence.sentence_id)).first()[0]
        connection.execute(
            'UPDATE sentence SET booking_id = {} '
            'WHERE sentence_id = {}'.format(
                booking_id, sentence.sentence_id))
    # Remove dummy default value, which should no longer be used
    op.alter_column('sentence', 'booking_id', server_default=None)

    op.create_foreign_key(None, 'sentence', 'booking', ['booking_id'], ['booking_id'])

    # Populate with dummy default value to avoid integrity error around
    # non-null constraint (will be overwritten after)
    op.add_column(
        'sentence_history',
        sa.Column(
            'booking_id', sa.Integer(), nullable=False, server_default='0'))
    sentence_history_table_view = sa.Table(
        'sentence_history',
        JailsBase.metadata,
        sa.Column('sentence_history_id', sa.Integer(), primary_key=True),
        sa.Column('sentence_id', sa.Integer()),
        sa.Column('booking_id', sa.Integer()),
        extend_existing=True)
    for sentence_history in connection.execute(
            sentence_history_table_view.select()):
        # Get any charge history that links to this sentence, since every
        # charge history snapshot will have the same booking ID. Use charge
        # history instead of charge since even sentences that are orphaned on
        # master will still be pointed to from charge history.
        booking_id = connection.execute(
            'SELECT charge_history.booking_id FROM charge_history WHERE '
            'charge_history.sentence_id = {} LIMIT 1'.format(
                sentence_history.sentence_id)).first()[0]
        connection.execute(
            'UPDATE sentence_history SET booking_id = {} '
            'WHERE sentence_history_id = {}'.format(
                booking_id, sentence_history.sentence_history_id))
    # Remove dummy default value, which should no longer be used
    op.alter_column('sentence_history', 'booking_id', server_default=None)

def downgrade():
    raise NotImplementedError()
