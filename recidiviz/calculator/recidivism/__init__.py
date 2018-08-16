# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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


"""The recidivism calculation pipeline.

This includes infrastructure, logic, and models for calculating recidivism
metrics from ingested records and persisting those metrics.
"""

from recidiviz.calculator.recidivism import identifier
from recidiviz.calculator.recidivism import pipeline
from recidiviz.calculator.recidivism import recidivism_event
from recidiviz.calculator.recidivism.metrics import RecidivismMetric
