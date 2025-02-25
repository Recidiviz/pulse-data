# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
from typing import Optional, List

from flask import Flask
from opencensus.trace.samplers import Sampler
from opencensus.trace.base_exporter import Exporter
from opencensus.trace.propagation.google_cloud_format import GoogleCloudFormatPropagator

class FlaskMiddleware:
    def __init__(
        self,
        app: Optional[Flask] = None,
        excludelist_paths: Optional[List[str]] = None,
        sampler: Optional[Sampler] = None,
        exporter: Optional[Exporter] = None,
        propagator: Optional[GoogleCloudFormatPropagator] = None,
    ) -> None: ...
