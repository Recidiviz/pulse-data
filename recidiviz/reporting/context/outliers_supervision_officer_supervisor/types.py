# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Types for OutliersSupervisionOfficerSupervisor report"""
from typing import List, Optional

import attr


@attr.s
class MetricHighlightDetail:
    condition: str = attr.ib()
    metrics: str = attr.ib()


@attr.s
class MultipleMetricHighlight:
    name: str = attr.ib()
    details: List[MetricHighlightDetail] = attr.ib()


@attr.s
class Highlights:
    multiple_metrics: Optional[List[MultipleMetricHighlight]] = attr.ib()
    no_outliers: Optional[str] = attr.ib()


@attr.s
class Faq:
    text: str = attr.ib()
    url: str = attr.ib()
