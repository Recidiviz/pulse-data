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
"""Functionality for transforming dimension values """

from typing import Callable, Dict, List, Union

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension

dimension_transformers: Dict[Dimension, Callable] = {}


def default_transformer(value: List[Union[str, int]]) -> List[Union[str, int]]:
    return value


def register_dimension_transformer(
    dimension: Dimension, value_transformer: Callable
) -> None:
    dimension_transformers[dimension] = value_transformer


def get_dimension_transformer(dimension: Dimension) -> Callable:
    return dimension_transformers.get(dimension, default_transformer)
