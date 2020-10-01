# pylint: skip-file
"""remove_assessment_levels

Revision ID: 7795ab9ba9e2
Revises: 15717deb3763
Create Date: 2020-09-30 18:41:04.406983

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7795ab9ba9e2'
down_revision = '15717deb3763'
branch_labels = None
depends_on = None

# With deprecated values
old_values = [
    'EXTERNAL_UNKNOWN',
    'LOW',
    'LOW_MEDIUM',
    'MEDIUM',
    'MEDIUM_HIGH',
    'MODERATE',
    'HIGH',
    'VERY_HIGH',
    'NOT_APPLICABLE',
    'UNDETERMINED'
]

# Without deprecated values
new_values = [
    'EXTERNAL_UNKNOWN',
    'LOW',
    'LOW_MEDIUM',
    'MEDIUM',
    'MEDIUM_HIGH',
    'MODERATE',
    'HIGH',
    'VERY_HIGH'
]


def upgrade():
    op.execute('ALTER TYPE state_assessment_level RENAME TO state_assessment_level_old;')
    sa.Enum(*new_values, name='state_assessment_level').create(bind=op.get_bind())
    op.alter_column('state_assessment', column_name='assessment_level',
                    type_=sa.Enum(*new_values, name='state_assessment_level'),
                    postgresql_using='assessment_level::text::state_assessment_level')
    op.alter_column('state_assessment_history', column_name='assessment_level',
                    type_=sa.Enum(*new_values, name='state_assessment_level'),
                    postgresql_using='assessment_level::text::state_assessment_level')
    op.execute('DROP TYPE state_assessment_level_old;')


def downgrade():
    op.execute('ALTER TYPE state_assessment_level RENAME TO state_assessment_level_old;')
    sa.Enum(*old_values, name='state_assessment_level').create(bind=op.get_bind())
    op.alter_column('state_assessment', column_name='assessment_level',
                    type_=sa.Enum(*old_values, name='state_assessment_level'),
                    postgresql_using='assessment_level::text::state_assessment_level')
    op.alter_column('state_assessment_history', column_name='assessment_level',
                    type_=sa.Enum(*old_values, name='state_assessment_level'),
                    postgresql_using='assessment_level::text::state_assessment_level')
    op.execute('DROP TYPE state_assessment_level_old;')
