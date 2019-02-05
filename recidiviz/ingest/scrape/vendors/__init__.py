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

""" Vendor classes that handle vendor specific navigation and data extraction.
All regions that use one of these vendors should inherit from the vendor
specific classes.
"""

from recidiviz.ingest.scrape.vendors.archonix.archonix_scraper import \
    ArchonixScraper
from recidiviz.ingest.scrape.vendors.iml.iml_scraper import ImlScraper
from recidiviz.ingest.scrape.vendors.net_data.net_data_scraper import \
    NetDataScraper
from recidiviz.ingest.scrape.vendors.newworld.newworld_scraper import \
    NewWorldScraper
import recidiviz.ingest.scrape.vendors.superion
