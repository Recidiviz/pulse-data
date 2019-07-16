"""court_type_to_str

Revision ID: 806d98096240
Revises: df3564c9f9fe
Create Date: 2019-03-21 16:38:17.265269

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = '806d98096240'
down_revision = 'df3564c9f9fe'
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    op.drop_column('charge', 'court_type')
    op.alter_column('charge', 'court_type_raw_text',
                    new_column_name='court_type')

    op.drop_column('charge_history', 'court_type')
    op.alter_column('charge_history', 'court_type_raw_text',
                    new_column_name='court_type')

    op.execute('DROP TYPE court_type;')


def downgrade():
    raise NotImplementedError()
