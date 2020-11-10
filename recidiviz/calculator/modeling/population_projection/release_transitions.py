# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""CompartmentTransitions instance with 'life sentence' long-sentence behavior"""

import pandas as pd

from recidiviz.calculator.modeling.population_projection.compartment_transitions import CompartmentTransitions


class ReleasedTransitions(CompartmentTransitions):
    """Simple compartment transitions for the Released compartment"""
    def normalize_long_sentences(self):
        self.long_sentence_transitions = pd.DataFrame({'remaining': [1]})
