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

import recidiviz.ingest.scrape.regions.us_fl_glades
import recidiviz.ingest.scrape.regions.us_al_autauga
import recidiviz.ingest.scrape.regions.us_al_cherokee
import recidiviz.ingest.scrape.regions.us_al_dale
import recidiviz.ingest.scrape.regions.us_al_dekalb
import recidiviz.ingest.scrape.regions.us_al_fayette
import recidiviz.ingest.scrape.regions.us_al_franklin
import recidiviz.ingest.scrape.regions.us_al_jackson
import recidiviz.ingest.scrape.regions.us_al_marion
import recidiviz.ingest.scrape.regions.us_al_pike
import recidiviz.ingest.scrape.regions.us_ar_baxter
import recidiviz.ingest.scrape.regions.us_ar_boone
import recidiviz.ingest.scrape.regions.us_ar_columbia
import recidiviz.ingest.scrape.regions.us_ar_craighead
import recidiviz.ingest.scrape.regions.us_ar_cross
import recidiviz.ingest.scrape.regions.us_ar_faulkner
import recidiviz.ingest.scrape.regions.us_ar_garland
import recidiviz.ingest.scrape.regions.us_ar_hempstead
import recidiviz.ingest.scrape.regions.us_ar_jefferson
import recidiviz.ingest.scrape.regions.us_ar_johnson
import recidiviz.ingest.scrape.regions.us_ar_lonoke
import recidiviz.ingest.scrape.regions.us_ar_marion
import recidiviz.ingest.scrape.regions.us_ar_monroe
import recidiviz.ingest.scrape.regions.us_ar_nevada
import recidiviz.ingest.scrape.regions.us_ar_poinsett
import recidiviz.ingest.scrape.regions.us_ar_saline
import recidiviz.ingest.scrape.regions.us_ar_st_francis
import recidiviz.ingest.scrape.regions.us_ar_stone
import recidiviz.ingest.scrape.regions.us_ar_van_buren
import recidiviz.ingest.scrape.regions.us_co_mesa
import recidiviz.ingest.scrape.regions.us_fl_alachua
import recidiviz.ingest.scrape.regions.us_fl_bradford
import recidiviz.ingest.scrape.regions.us_fl_columbia
import recidiviz.ingest.scrape.regions.us_fl_hendry
import recidiviz.ingest.scrape.regions.us_fl_martin
import recidiviz.ingest.scrape.regions.us_fl_nassau
import recidiviz.ingest.scrape.regions.us_fl_osceola
import recidiviz.ingest.scrape.regions.us_ga_berrien
import recidiviz.ingest.scrape.regions.us_ga_douglas
import recidiviz.ingest.scrape.regions.us_ga_floyd
import recidiviz.ingest.scrape.regions.us_ga_gwinnett
import recidiviz.ingest.scrape.regions.us_ga_lumpkin
import recidiviz.ingest.scrape.regions.us_ga_toombs
import recidiviz.ingest.scrape.regions.us_in_jackson
import recidiviz.ingest.scrape.regions.us_in_scott
import recidiviz.ingest.scrape.regions.us_in_vigo
import recidiviz.ingest.scrape.regions.us_ks_cherokee
import recidiviz.ingest.scrape.regions.us_ks_jefferson
import recidiviz.ingest.scrape.regions.us_ks_pratt
import recidiviz.ingest.scrape.regions.us_ky_greenup
import recidiviz.ingest.scrape.regions.us_ky_marion
import recidiviz.ingest.scrape.regions.us_ky_grant
import recidiviz.ingest.scrape.regions.us_ky_campbell
import recidiviz.ingest.scrape.regions.us_ky_harlan
import recidiviz.ingest.scrape.regions.us_mo_barry
import recidiviz.ingest.scrape.regions.us_mo_cape_girardeau
import recidiviz.ingest.scrape.regions.us_mo_johnson
import recidiviz.ingest.scrape.regions.us_mo_lawrence
import recidiviz.ingest.scrape.regions.us_mo_livingston
import recidiviz.ingest.scrape.regions.us_mo_morgan
import recidiviz.ingest.scrape.regions.us_mo_stone
import recidiviz.ingest.scrape.regions.us_mo_stone
import recidiviz.ingest.scrape.regions.us_ms_clay
import recidiviz.ingest.scrape.regions.us_ms_desoto
import recidiviz.ingest.scrape.regions.us_ms_kemper
import recidiviz.ingest.scrape.regions.us_ms_tunica
import recidiviz.ingest.scrape.regions.us_mt_gallatin
import recidiviz.ingest.scrape.regions.us_nc_alamance
import recidiviz.ingest.scrape.regions.us_nc_buncombe
import recidiviz.ingest.scrape.regions.us_nc_cabarrus
import recidiviz.ingest.scrape.regions.us_nc_cleveland
import recidiviz.ingest.scrape.regions.us_nc_forsyth
import recidiviz.ingest.scrape.regions.us_nc_guilford
import recidiviz.ingest.scrape.regions.us_nc_lincoln
import recidiviz.ingest.scrape.regions.us_nc_new_hanover
import recidiviz.ingest.scrape.regions.us_nc_rowan
import recidiviz.ingest.scrape.regions.us_nc_wake
import recidiviz.ingest.scrape.regions.us_nj_bergen
import recidiviz.ingest.scrape.regions.us_ny
import recidiviz.ingest.scrape.regions.us_ok_rogers
import recidiviz.ingest.scrape.regions.us_pa
import recidiviz.ingest.scrape.regions.us_pa_dauphin
import recidiviz.ingest.scrape.regions.us_pa_greene
import recidiviz.ingest.scrape.regions.us_tn_mcminn
import recidiviz.ingest.scrape.regions.us_tx_brown
import recidiviz.ingest.scrape.regions.us_tx_cochran
import recidiviz.ingest.scrape.regions.us_tx_coleman
import recidiviz.ingest.scrape.regions.us_tx_cooke
import recidiviz.ingest.scrape.regions.us_tx_erath
import recidiviz.ingest.scrape.regions.us_tx_freestone
import recidiviz.ingest.scrape.regions.us_tx_hockley
import recidiviz.ingest.scrape.regions.us_tx_hopkins
import recidiviz.ingest.scrape.regions.us_tx_liberty
import recidiviz.ingest.scrape.regions.us_tx_ochiltree
import recidiviz.ingest.scrape.regions.us_tx_red_river
import recidiviz.ingest.scrape.regions.us_tx_rusk
import recidiviz.ingest.scrape.regions.us_tx_titus
import recidiviz.ingest.scrape.regions.us_tx_tom_green
import recidiviz.ingest.scrape.regions.us_tx_upshur
import recidiviz.ingest.scrape.regions.us_tx_van_zandt
import recidiviz.ingest.scrape.regions.us_tx_wichita
import recidiviz.ingest.scrape.regions.us_tx_wilson
import recidiviz.ingest.scrape.regions.us_tx_young
import recidiviz.ingest.scrape.regions.us_vt
import recidiviz.ingest.scrape.worker
