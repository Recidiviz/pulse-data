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

"""The ingest portion of the Recidiviz data platform.

This includes infrastructure, logic, and models for ingesting, validating,
normalizing, and storing records ingested from various criminal justice data
sources.
"""

import recidiviz.ingest.us_ar_van_buren
import recidiviz.ingest.us_co_mesa
import recidiviz.ingest.us_fl_hendry
import recidiviz.ingest.us_fl_nassau
import recidiviz.ingest.us_fl_martin
import recidiviz.ingest.us_fl_osceola
import recidiviz.ingest.us_ms_desoto
import recidiviz.ingest.us_mt_gallatin
import recidiviz.ingest.us_mo_stone
import recidiviz.ingest.us_nj_bergen
import recidiviz.ingest.us_nc_buncombe
import recidiviz.ingest.us_nc_guilford
import recidiviz.ingest.us_ny
import recidiviz.ingest.us_pa
import recidiviz.ingest.us_pa_dauphin
import recidiviz.ingest.us_pa_greene
import recidiviz.ingest.us_tx_brown
import recidiviz.ingest.us_tx_cooke
import recidiviz.ingest.us_vt
import recidiviz.ingest.worker
