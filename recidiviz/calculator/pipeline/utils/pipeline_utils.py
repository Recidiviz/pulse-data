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
"""Helper functions for the pipeline.py file of each pipeline."""
import json
from typing import Dict, Tuple, Any, Optional
from enum import Enum

from recidiviz.calculator.pipeline.utils.metric_utils import json_serializable_metric_key


def tagged_metric_output(metric_combination: Tuple[Dict[str, Any], Any], metric_type_output_tags: Dict[Enum, str]) -> \
        Optional[Tuple[str, Tuple[str, Any]]]:
    """Converts the given metric_combination into a JSON string representation of the metric, and packages it with the
     value associated with the metric. If there's a relevant output tag for the metric_type in the metric_key, then
     returns that output tag along with the packaged metric and value."""
    metric_key, value = metric_combination
    metric_type = metric_key.get('metric_type')

    if isinstance(metric_type, Enum):
        is_person_level_metric = metric_key.get('person_id') is not None

        # Converting the metric key to a JSON string so it is hashable
        serializable_dict = json_serializable_metric_key(metric_key)
        json_key = json.dumps(serializable_dict, sort_keys=True)

        output = (json_key, value)
        output_tag = 'person_level_output' if is_person_level_metric else metric_type_output_tags.get(metric_type)

        if output_tag:
            return output_tag, output

    return None
