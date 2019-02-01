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

import recidiviz.ingest.us_al_autauga
import recidiviz.ingest.us_al_cherokee
import recidiviz.ingest.us_al_dale
import recidiviz.ingest.us_al_dekalb
import recidiviz.ingest.us_al_fayette
import recidiviz.ingest.us_al_franklin
import recidiviz.ingest.us_al_jackson
import recidiviz.ingest.us_al_marion
import recidiviz.ingest.us_al_pike
import recidiviz.ingest.us_ar_boone
import recidiviz.ingest.us_ar_craighead
import recidiviz.ingest.us_ar_faulkner
import recidiviz.ingest.us_ar_garland
import recidiviz.ingest.us_ar_hempstead
import recidiviz.ingest.us_ar_jefferson
import recidiviz.ingest.us_ar_johnson
import recidiviz.ingest.us_ar_lonoke
import recidiviz.ingest.us_ar_marion
import recidiviz.ingest.us_ar_monroe
import recidiviz.ingest.us_ar_nevada
import recidiviz.ingest.us_ar_poinsett
import recidiviz.ingest.us_ar_saline
import recidiviz.ingest.us_ar_stone
import recidiviz.ingest.us_ar_van_buren
import recidiviz.ingest.us_co_mesa
import recidiviz.ingest.us_fl_bradford
import recidiviz.ingest.us_fl_columbia
import recidiviz.ingest.us_fl_hendry
import recidiviz.ingest.us_fl_nassau
import recidiviz.ingest.us_fl_martin
import recidiviz.ingest.us_fl_osceola
import recidiviz.ingest.us_ga_berrien
import recidiviz.ingest.us_ga_douglas
import recidiviz.ingest.us_ga_gwinnett
import recidiviz.ingest.us_in_jackson
import recidiviz.ingest.us_in_scott
import recidiviz.ingest.us_in_vigo
import recidiviz.ingest.us_ks_cherokee
import recidiviz.ingest.us_ks_jefferson
import recidiviz.ingest.us_ks_pratt
import recidiviz.ingest.us_mo_stone
import recidiviz.ingest.us_ms_clay
import recidiviz.ingest.us_ms_kemper
import recidiviz.ingest.us_ms_tunica
import recidiviz.ingest.us_ms_desoto
import recidiviz.ingest.us_mt_gallatin
import recidiviz.ingest.us_mo_barry
import recidiviz.ingest.us_mo_cape_girardeau
import recidiviz.ingest.us_mo_johnson
import recidiviz.ingest.us_mo_lawrence
import recidiviz.ingest.us_mo_livingston
import recidiviz.ingest.us_mo_morgan
import recidiviz.ingest.us_mo_stone
import recidiviz.ingest.us_nc_alamance
import recidiviz.ingest.us_nj_bergen
import recidiviz.ingest.us_nc_buncombe
import recidiviz.ingest.us_nc_guilford
import recidiviz.ingest.us_ny
import recidiviz.ingest.us_ok_rogers
import recidiviz.ingest.us_pa
import recidiviz.ingest.us_pa_dauphin
import recidiviz.ingest.us_pa_greene
import recidiviz.ingest.us_tn_mcminn
import recidiviz.ingest.us_tx_brown
import recidiviz.ingest.us_tx_cochran
import recidiviz.ingest.us_tx_coleman
import recidiviz.ingest.us_tx_cooke
import recidiviz.ingest.us_tx_erath
import recidiviz.ingest.us_tx_freestone
import recidiviz.ingest.us_tx_hockley
import recidiviz.ingest.us_tx_hopkins
import recidiviz.ingest.us_tx_liberty
import recidiviz.ingest.us_tx_ochiltree
import recidiviz.ingest.us_tx_red_river
import recidiviz.ingest.us_tx_rusk
import recidiviz.ingest.us_tx_titus
import recidiviz.ingest.us_tx_upshur
import recidiviz.ingest.us_tx_van_zandt
import recidiviz.ingest.us_tx_wilson
import recidiviz.ingest.us_tx_wichita
import recidiviz.ingest.us_tx_young
import recidiviz.ingest.us_vt
import recidiviz.ingest.worker
