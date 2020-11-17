# pylint: skip-file
"""add_alias_type

Revision ID: 7ab233533ebe
Revises: 9cf0a4084c25
Create Date: 2019-09-04 13:09:43.684623

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7ab233533ebe'
down_revision = '9cf0a4084c25'
branch_labels = None
depends_on = None

alias_type_values = ['AFFILIATION_NAME', 'ALIAS', 'GIVEN_NAME', 'MAIDEN_NAME', 'NICKNAME']


def upgrade():
    # Create the new enum type first
    sa.Enum(*alias_type_values, name='state_person_alias_type').create(bind=op.get_bind())

    # Now reference that new type in the appropriate columns
    op.add_column('state_person_alias', sa.Column('alias_type', sa.Enum(*alias_type_values, name='state_person_alias_type'), nullable=True))
    op.add_column('state_person_alias', sa.Column('alias_type_raw_text', sa.String(length=255), nullable=True))
    op.add_column('state_person_alias_history', sa.Column('alias_type', sa.Enum(*alias_type_values, name='state_person_alias_type'), nullable=True))
    op.add_column('state_person_alias_history', sa.Column('alias_type_raw_text', sa.String(length=255), nullable=True))


def downgrade():
    op.drop_column('state_person_alias_history', 'alias_type_raw_text')
    op.drop_column('state_person_alias_history', 'alias_type')
    op.drop_column('state_person_alias', 'alias_type_raw_text')
    op.drop_column('state_person_alias', 'alias_type')

    op.execute('DROP TYPE state_person_alias_type;')
