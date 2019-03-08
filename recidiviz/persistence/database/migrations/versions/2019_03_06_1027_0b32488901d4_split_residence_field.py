"""split_residence_field

Revision ID: 0b32488901d4
Revises: a1da0fc514a7
Create Date: 2019-03-06 10:27:43.741301

"""

import re

from alembic import op
import sqlalchemy as sa
from uszipcode import SearchEngine


# revision identifiers, used by Alembic.
revision = '0b32488901d4'
down_revision = 'a1da0fc514a7'
branch_labels = None
depends_on = None


zip_code_search = SearchEngine(simple_zipcode=True)
county_suffixes = ['CITY', 'COUNTY', 'PARISH', 'MUNICIPIO', 'CENSUS AREA', 'CITY AND BOROUGH', 'BOROUGH']
residency_status_values = ['PERMANENT', 'TRANSIENT', 'HOMELESS']


# Returns true if true, false if false, and None if cannot be determined
def get_is_resident(place_of_residence, region):
    # Remove 'us_xx_' prefix
    region_county = region[6:]
    # Replace underscores with spaces because uszipcode uses spaces
    normalized_region_county = region_county.upper().replace('_', ' ')

    zip_code = None
    zip_code_matches = re.findall(r'\d{5}', place_of_residence)
    if not zip_code_matches:
        return None
    # If more than one match is present, take the last one (to account for
    # cases where there is a 5-digit address)
    zip_code = zip_code_matches[-1]

    residence_county = zip_code_search.by_zipcode(zip_code).county
    if not residence_county:
        return None

    # uszipcode county names only contain hyphens and periods as special
    # characters
    normalized_residence_county = \
        residence_county.upper().replace('-', ' ').replace('.', '')

    # Compare region county to base county name and to county name with any
    # matching suffixes stripped
    possible_county_names = {normalized_residence_county}
    for suffix in county_suffixes:
        suffix_length = len(suffix)
        if normalized_residence_county[-(suffix_length):] == suffix:
            possible_county_names.add(
                normalized_residence_county[:-(suffix_length + 1)])
    for county_name in possible_county_names:
        if normalized_region_county == county_name:
            return True
    return False


# Returns residency status if it can be determined, and None otherwise
def get_residency_status(place_of_residence):
    if not place_of_residence:
        return None
    normalized_residence = place_of_residence.upper()
    if 'HOMELESS' in normalized_residence:
        return 'HOMELESS'
    if 'TRANSIENT' in normalized_residence:
        return 'TRANSIENT'
    return 'PERMANENT'


def upgrade():
    connection = op.get_bind()

    op.add_column(
        'person',
        sa.Column('resident_of_region', sa.Boolean(), nullable=True))
    op.add_column(
        'person_history',
        sa.Column('resident_of_region', sa.Boolean(), nullable=True))

    sa.Enum(*residency_status_values, name='residency_status') \
        .create(bind=op.get_bind())

    op.add_column(
        'person',
        sa.Column(
            'residency_status',
            sa.Enum(*residency_status_values, name='residency_status'),
            nullable=True))
    op.add_column(
        'person_history',
        sa.Column(
            'residency_status',
            sa.Enum(*residency_status_values, name='residency_status'),
            nullable=True))

    person_results = connection.execute(
        'SELECT person_id, place_of_residence, region '
        'FROM person WHERE place_of_residence IS NOT NULL;').fetchall()
    person_history_results = connection.execute(
        'SELECT person_history_id, place_of_residence, region '
        'FROM person_history '
        'WHERE place_of_residence IS NOT NULL;').fetchall()

    for person_id, place_of_residence, region in person_results:
        is_resident = get_is_resident(place_of_residence, region)
        residency_status = get_residency_status(place_of_residence)
        if is_resident is not None:
            connection.execute(
                'UPDATE person SET resident_of_region = {} WHERE '
                'person_id = {};'.format(
                    str(is_resident).upper(), person_id))
        if residency_status is not None:
            connection.execute(
                'UPDATE person SET residency_status = \'{}\' WHERE '
                'person_id = {};'.format(
                    residency_status, person_id))
                
    for person_history_id, place_of_residence, region in \
            person_history_results:
        is_resident = get_is_resident(place_of_residence, region)
        residency_status = get_residency_status(place_of_residence)
        if is_resident is not None:
            connection.execute(
                'UPDATE person_history SET resident_of_region = {} WHERE '
                'person_history_id = {};'.format(
                    str(is_resident).upper(), person_history_id))
        if residency_status is not None:
            connection.execute(
                'UPDATE person_history SET residency_status = \'{}\' WHERE '
                'person_history_id = {};'.format(
                    residency_status, person_history_id))


    op.drop_column('person', 'place_of_residence')
    op.drop_column('person_history', 'place_of_residence')


def downgrade():
    raise NotImplementedError()

