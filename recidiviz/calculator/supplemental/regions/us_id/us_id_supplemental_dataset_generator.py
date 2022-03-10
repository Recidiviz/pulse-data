# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains the supplemental datasets for US_ID"""

from recidiviz.calculator.supplemental.regions.us_id.us_id_case_note_matched_entities import (
    UsIdCaseNoteMatchedEntities,
)
from recidiviz.calculator.supplemental.supplemental_dataset import (
    StateSpecificSupplementalDatasetGenerator,
)


# TODO(#11312): Remove once dataflow pipeline is deployed.
class UsIdSupplementalDatasetGenerator(StateSpecificSupplementalDatasetGenerator):
    """Contains the supplemental datasets for US_ID"""

    def __init__(self) -> None:
        super().__init__(supplemental_dataset_tables=[UsIdCaseNoteMatchedEntities()])  # type: ignore
