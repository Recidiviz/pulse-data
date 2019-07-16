"""migrate_frontslash_enums

Revision ID: c82f37920d63
Revises: ade09190b367
Create Date: 2019-03-01 11:25:44.311952

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c82f37920d63'
down_revision = 'ade09190b367'
branch_labels = None
depends_on = None


new_values = ['AMERICAN_INDIAN_ALASKAN_NATIVE', 'ASIAN', 'BLACK', 'EXTERNAL_UNKNOWN', 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER', 'OTHER', 'WHITE']


def upgrade():
    op.execute('ALTER TYPE race RENAME TO race_old;')

    op.alter_column('person', 'race', new_column_name='race_old')
    op.alter_column('person_history', 'race', new_column_name='race_old')

    sa.Enum(*new_values, name='race').create(bind=op.get_bind())

    op.add_column(
        'person',
        sa.Column(
            'race',
            sa.Enum(*new_values, name='race'),
            nullable=True))
    op.add_column(
        'person_history',
        sa.Column(
            'race',
            sa.Enum(*new_values, name='race'),
            nullable=True))

    op.execute('UPDATE person SET race = race_old::text::race '
               'WHERE race_old NOT IN '
               '(\'AMERICAN_INDIAN/ALASKAN_NATIVE\', '
               '\'NATIVE_HAWAIIAN/PACIFIC_ISLANDER\');')
    op.execute('UPDATE person SET race = \'AMERICAN_INDIAN_ALASKAN_NATIVE\' '
               'WHERE race_old = \'AMERICAN_INDIAN/ALASKAN_NATIVE\';')
    op.execute('UPDATE person SET race = \'NATIVE_HAWAIIAN_PACIFIC_ISLANDER\' '
               'WHERE race_old = \'NATIVE_HAWAIIAN/PACIFIC_ISLANDER\';')

    op.execute('UPDATE person_history SET race = race_old::text::race '
               'WHERE race_old NOT IN '
               '(\'AMERICAN_INDIAN/ALASKAN_NATIVE\', '
               '\'NATIVE_HAWAIIAN/PACIFIC_ISLANDER\');')
    op.execute('UPDATE person_history '
               'SET race = \'AMERICAN_INDIAN_ALASKAN_NATIVE\' '
               'WHERE race_old = \'AMERICAN_INDIAN/ALASKAN_NATIVE\';')
    op.execute('UPDATE person_history '
               'SET race = \'NATIVE_HAWAIIAN_PACIFIC_ISLANDER\' '
               'WHERE race_old = \'NATIVE_HAWAIIAN/PACIFIC_ISLANDER\';')

    op.drop_column('person', 'race_old')
    op.drop_column('person_history', 'race_old')

    op.execute('DROP TYPE race_old')


def downgrade():
    raise NotImplementedError()
