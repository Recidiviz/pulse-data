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
"""
A script to initialize a TextAnalyzer for US_ID text entities.

This can be imported within a python environment via:
    from recidiviz.tools.analyst.us_id_text_analyzer import *

Which will initialize the `analyzer` object. One can then run functions like
    analyzer.extract_entities("SOME TEXT")
"""

from recidiviz.common.text_analysis import TextAnalyzer, TextMatchingConfiguration
from recidiviz.pipelines.supplemental.us_id_case_note_extracted_entities.us_id_text_analysis_configuration import (
    UsIdTextEntity,
)

analyzer = TextAnalyzer(
    TextMatchingConfiguration(
        stop_words_to_remove={"in", "out"}, text_entities=list(UsIdTextEntity)
    )
)
