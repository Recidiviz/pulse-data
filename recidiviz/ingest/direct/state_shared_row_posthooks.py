# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Row posthooks shared between multiple direct ingest controllers.

All functions added to this file should have an associated to-do outlining how
we might update our yaml mapping support to handle this case more generally.
"""
from typing import Dict, List, Callable

from recidiviz.common.constants.state.state_person_alias import \
    StatePersonAliasType
from recidiviz.ingest.direct.direct_ingest_controller_utils import \
    create_if_not_exists
from recidiviz.ingest.models.ingest_info import IngestObject, StateAlias, \
    StatePerson, StatePersonExternalId
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


# TODO(1882): This should no-longer be necessary once you can map a column
#  value to multiple fields on the ingested object.
def copy_name_to_alias(_file_tag: str,
                       _row: Dict[str, str],
                       extracted_objects: List[IngestObject],
                       _cache: IngestObjectCache):
    """Copy all name fields stored on a StatePerson object to a new StateAlias
    child object.
    """
    for extracted_object in extracted_objects:
        if isinstance(extracted_object, StatePerson):
            alias_to_create = StateAlias(
                full_name=extracted_object.full_name,
                surname=extracted_object.surname,
                given_names=extracted_object.given_names,
                middle_names=extracted_object.middle_names,
                name_suffix=extracted_object.name_suffix,
                alias_type=StatePersonAliasType.GIVEN_NAME.value
            )

            create_if_not_exists(alias_to_create,
                                 extracted_object,
                                 'state_aliases')


# TODO(1882): If yaml format supported raw values, this would no-longer be
#  necessary.
def gen_label_single_external_id_hook(external_id_type: str) -> Callable:
    """Generates a row post-hook that will hydrate the id_type field on the
    singular StatePersonExternalId in the extracted objects. Will throw if
    there is more than one external id to label.
    """

    def _label_external_id(_file_tag: str,
                           _row: Dict[str, str],
                           extracted_objects: List[IngestObject],
                           _cache: IngestObjectCache):
        found = False
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePersonExternalId):
                if found:
                    raise ValueError(
                        'Already found object of type StatePersonExternalId')

                extracted_object.__setattr__('id_type', external_id_type)
                found = True

    return _label_external_id
