# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Contains all available Public Pathways metrics."""
import os

from recidiviz.case_triage.shared_pathways.metric_query_builder_config_parser import (
    MetricQueryBuilderConfigParser,
)
from recidiviz.persistence.database.schema_type import SchemaType

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

config_parser = MetricQueryBuilderConfigParser(config_path, SchemaType.PUBLIC_PATHWAYS)
ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME = config_parser.parse()
ALL_PUBLIC_PATHWAYS_METRICS = list(ALL_PUBLIC_PATHWAYS_METRICS_BY_NAME.values())
