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
"""Utility functions related to vendors"""
import os
import pkgutil
from typing import Any, Dict, Optional, Set

import yaml

from recidiviz.ingest.scrape import vendors

BASE_VENDOR_PATH = os.path.dirname(vendors.__file__)


def get_vendors() -> Set[str]:
    return {
        vendor_module.name for vendor_module in pkgutil.iter_modules([BASE_VENDOR_PATH])
    }


def get_vendor_queue_params(vendor: str) -> Optional[Dict[str, Any]]:
    """Gets the queue params for the given region.

    Returns:
        - None, if queue.yaml does not exist (and a queue should not be created)
        - dict, if queue.yaml does exist (and a queue should be created)
    """
    queue_param_path = os.path.join(BASE_VENDOR_PATH, vendor, "queue.yaml")
    if not os.path.exists(queue_param_path):
        return None
    with open(queue_param_path, encoding="utf-8") as queue_params:
        return yaml.full_load(queue_params) or {}
