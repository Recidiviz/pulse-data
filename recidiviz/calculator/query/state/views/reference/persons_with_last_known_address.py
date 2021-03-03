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
"""Every person with their last known address that is not a prison facility or a P&P office."""
# pylint: disable=trailing-whitespace
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config

PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_NAME = "persons_with_last_known_address"

PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_DESCRIPTION = """Persons with their last known address that is not a prison facility or a P&P office."""

# TODO(#2843): Update to support multiple states
PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_QUERY_TEMPLATE = """
    /*{description}*/

    SELECT state_code, person.person_id, last_known_address
    FROM
    `{project_id}.{base_dataset}.state_person` person
    LEFT JOIN
    (SELECT person_id, last_known_address
    FROM
    (SELECT person_id,
    current_address as last_known_address,
    row_number() OVER (PARTITION BY person_id ORDER BY valid_from DESC) as recency_rank
    FROM
    `{project_id}.{base_dataset}.state_person_history`
    WHERE
    # Known ND DOCR facilities
    (current_address NOT LIKE '%3100 RAILROAD AVE%'
    AND current_address NOT LIKE '%NDSP%ND%'
    AND current_address NOT LIKE '%PO BOX 5521%ND%'
    AND current_address NOT LIKE '%440 MCKENZIE ST%ND%'
    AND current_address NOT LIKE '%ABSCOND%ND%'
    AND current_address NOT LIKE '%250 N 31ST%ND%'
    AND current_address NOT LIKE '%461 34TH ST S%ND%'
    AND current_address NOT LIKE '%1600 2ND AVE SW%ND%'
    AND current_address NOT LIKE '%702 1ST AVE S%ND%'
    AND current_address NOT LIKE '%311 S 4TH ST STE 101%ND%'
    AND current_address NOT LIKE '%222 WALNUT ST W%ND%'
    AND current_address NOT LIKE '%709 DAKOTA AVE STE D%ND%'
    AND current_address NOT LIKE '%113 MAIN AVE E STE B%ND%'
    AND current_address NOT LIKE '%712 5TH AVE%ND%'
    AND current_address NOT LIKE '%705 EAST HIGHLAND DR. SUITE B%ND%'
    AND current_address NOT LIKE '%705 E HIGHLAND DR STE B%ND%'
    AND current_address NOT LIKE '%135 SIMS ST STE 205%ND%'
    AND current_address NOT LIKE '%638 COOPER AVE%ND%'
    AND current_address NOT LIKE '%206 MAIN ST W%ND%'
    AND current_address NOT LIKE '%519 MAIN ST STE 8%ND%'
    AND current_address NOT LIKE '%115 S 5TH ST STE A%ND%'
    AND current_address NOT LIKE '%117 HWY 49%ND%')

    -- General filter for addresses that seem to be correctional facilities of some sort
    AND current_address NOT LIKE '%JAIL%'
    AND current_address NOT LIKE '%PRISON%'
    AND current_address NOT LIKE '%CCC%'
    AND current_address NOT LIKE '%PENITENTIARY%'
    )
    WHERE recency_rank = 1) people_with_last_known_address
    ON person.person_id = people_with_last_known_address.person_id
    ORDER BY person_id ASC
    """

PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_NAME,
    view_query_template=PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_QUERY_TEMPLATE,
    description=PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_BUILDER.build_and_print()
