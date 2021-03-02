# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Tools for working with regions.

Regions represent geographic areas/legal jurisdictions from which we ingest
criminal justice data and calculate metrics.
"""
import importlib
import os
import pkgutil
from datetime import datetime, tzinfo
from enum import Enum, auto
from itertools import chain
from types import ModuleType
from typing import Any, Dict, Optional, Set
from typing import List

import attr
import pytz
import yaml

from recidiviz.common import fips
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.utils import environment
from recidiviz.ingest.scrape import regions as scraper_regions_module
from recidiviz.ingest.direct import regions as direct_ingest_regions_module
from recidiviz.utils.environment import GCPEnvironment


class RemovedFromWebsite(Enum):
    RELEASED = 'RELEASED'
    UNKNOWN_SIGNIFICANCE = 'UNKNOWN_SIGNIFICANCE'


class IngestType(Enum):
    DIRECT_INGEST = auto()
    SCRAPER = auto()


# Cache of the `Region` objects.
REGIONS: Dict[IngestType, Dict[str, 'Region']] = {}


def _to_lower(s: str) -> str:
    return s.lower()


@attr.s(frozen=True)
class Region:
    """Constructs region entity with attributes and helper functions

    Builds a region entity, which holds useful info about a region as properties
    and has helper functions to get region-specific configuration info.

    Attributes:
        region_code: (string) Region code
        agency_name: (string) Human-readable agency name
        agency_type: (string) 'prison' or 'jail'
        environment: (string) The environment the region is allowed to run in.
        base_url: (string) Base URL for scraping
        should_proxy: (string) Whether or not to send requests through the proxy
        timezone: (string) Timezone in which this region resides. If the region
            is in multiple timezones, this is the timezone in which most of the
            region resides, where "most" is whatever is most useful for that
            region, e.g. population count versus land size.
        shared_queue: (string) The name of an existing queue to use instead of
            creating one for this region.
        queue: (dict) Any parameters to override when creating the queue for
            this region.
        removed_from_website: (string) Value to use when a person is removed
            from a website (converted to `RemovedFromWebsite`).
        names_file: (string) Optional filename of names file for this region
        is_stoppable: (string) Whether or not this region is stoppable via the
            cron job /scraper/stop.
        stripe: (string) Stripe to which this region belongs to. This is used
            further divide up a timezone
        facility_id: (string) Default facility ID for region
    """

    region_code: str = attr.ib(converter=_to_lower)
    jurisdiction_id: str = attr.ib(validator=attr.validators.instance_of(str))
    agency_name: str = attr.ib()
    agency_type: str = attr.ib()
    timezone: tzinfo = attr.ib(converter=pytz.timezone)
    region_module: ModuleType = attr.ib(default=None)
    environment: Optional[str] = attr.ib(default=None)
    base_url: Optional[str] = attr.ib(default=None)
    shared_queue: Optional[str] = attr.ib(default=None)
    queue: Optional[Dict[str, Any]] = attr.ib(default=None)
    removed_from_website: RemovedFromWebsite = \
        attr.ib(default=RemovedFromWebsite.RELEASED,
                converter=RemovedFromWebsite)
    names_file: Optional[str] = attr.ib(default=None)
    should_proxy: Optional[bool] = attr.ib(default=False)
    is_stoppable: Optional[bool] = attr.ib(default=False)
    is_direct_ingest: Optional[bool] = attr.ib(default=False)
    stripe: Optional[str] = attr.ib(default="0")
    facility_id: Optional[str] = attr.ib(default=None)

    # TODO(#3162): Once SQL preprocessing flow is enabled for all direct ingest regions, delete these configs
    raw_vs_ingest_file_name_differentiation_enabled_env = attr.ib(default=None)
    raw_data_bq_imports_enabled_env = attr.ib(default=None)
    ingest_view_exports_enabled_env = attr.ib(default=None)

    @region_code.validator
    def validate_region_code(self, _attr: attr.Attribute, region_code: str) -> None:
        fips.validate_county_code(region_code)

    def __attrs_post_init__(self):
        if self.queue and self.shared_queue:
            raise ValueError(
                'Only one of `queue` and `shared_queue` can be set.')
        if self.environment not in {*environment.GCP_ENVIRONMENTS, None}:
            raise ValueError('Invalid environment: {}'.format(self.environment))
        if self.facility_id and len(self.facility_id) != 16:
            raise ValueError(f'Improperly formatted FID [{self.facility_id}], should be length 16')

    def get_ingestor_class(self):
        """Retrieve the class for the ingest object for a particular region

        Returns:
            An instance of the region's ingest class (e.g., UsNyScraper)
        """
        ingest_type_name = 'Controller' if self.is_direct_ingest else 'Scraper'

        module_name = \
            f'{self.region_module.__name__}.{self.region_code}' \
            f'.{self.region_code}_{ingest_type_name.lower()}'

        module = importlib.import_module(module_name)
        ingest_class = getattr(
            module, get_ingestor_name(self.region_code, ingest_type_name))

        return ingest_class

    def get_ingestor(self):
        """Retrieve an ingest object for a particular region

        Returns:
            An instance of the region's ingest class (e.g., UsNyScraper)
        """
        ingest_class = self.get_ingestor_class()

        return ingest_class()

    def get_enum_overrides(self) -> EnumOverrides:
        """Retrieves the overrides object of a region"""
        obj = self.get_ingestor()
        if obj:
            return obj.get_enum_overrides()
        return EnumOverrides.empty()

    def get_queue_name(self) -> str:
        """Returns the name of the queue to be used for the region"""
        return self.shared_queue if self.shared_queue \
            else '{}-scraper-v2'.format(self.region_code.replace('_', '-'))

    def is_ingest_launched_in_env(self) -> bool:
        """Returns true if ingest can be launched for this region in the current
        environment.

        If we are in prod, the region config must be explicitly set to specify
        this region can be run in prod. All regions can be triggered to run in
        staging.
        """
        return not environment.in_gcp_production() \
            or self.environment == environment.get_gcp_environment()

    def is_ingest_launched_in_production(self) -> bool:
        """Returns true if ingest can be launched for this region in production. """
        return self.environment is not None and self.environment.lower() == GCPEnvironment.PRODUCTION.value.lower()

    def is_raw_vs_ingest_file_name_detection_enabled(self) -> bool:
        """Returns True if this is ready for ingest to differentiate between files with the 'raw' and 'ingest_view'
        file types in the file name.

        Side effects when enabled:
        - When new, un-normalized files are dropped in the region's ingest bucket, the file name will be normalized, now
            with the file type 'raw' added to the name.
        - Split files will always get created with normalized names with type 'ingest_view'
        - Ingest file prioritizer will only look at 'ingest_view' type files. We will not move a file with 'raw' in
            the name through the pre-existing ingest flow.
        - Files with 'ingest_view' type that have been ingested to Postgres will be moved to
            <storage-bucket>/<region-code>/ingest_view/<year>/<month>/<day>/ subdirectory
        - If a 'raw' file is not in the raw data yaml for this region, we will ignore it after normalizing. Otherwise:
        - If are_raw_data_bq_imports_enabled_in_env() is not True, we will leave this file as 'unprocessed' in the
            region ingest bucket. If it is False, we will upload the raw file to BQ raw tables.

        Conditions to enable for region:
        - Existing normalized files in storage or ingest buckets must be moved to include either 'raw' or 'ingest_view'
            file type in the names.
        - Any "derived", ingest-ready files (i.e. based on a SQL query on several tables) that get manually uploaded to
            the bucket after this is enabled must have a pre-normalized name with 'ingest_view' file type.
        - We are prepared to manually upload ingest-ready files (MO, ID, PA, any other new states) or we are ready to
            fully enable raw data imports (ND, other launched direct ingest counties).

        If the |raw_vs_ingest_file_name_differentiation_enabled_env| config is unset, returns False. If it is set to
        'prod', this will also be enabled in staging.
        """
        return self.raw_vs_ingest_file_name_differentiation_enabled_env is not None and \
            (not environment.in_gcp_production() or
             self.raw_vs_ingest_file_name_differentiation_enabled_env == environment.get_gcp_environment())

    def are_raw_data_bq_imports_enabled_in_env(self) -> bool:
        """Returns true if this regions supports raw data import to BQ.

        Side effects when enabled:
        - For this region, we will create a us_xx_raw_data BQ dataset on launch to store raw data tables for that region
            (if it does not already exist).
        - For this region, we will create a us_xx_raw_data_up_to_date_views BQ dataset on launch to store raw data
            tables for that region (if it does not already exist).
        - For every file tag in the region raw data config, auto generate <raw_data_table_name>_by_update_date and
            <raw_data_table_name>_latest on launch.
        - Every 'raw' file we encounter that also matches a tag in the raw data yaml config for this region will get
            uploaded to a BQ raw data table (table will be auto-created if it does not exist)
        - When a raw file is uploaded to BQ, we will update the raw_file_metadata table with information about this
            file.
        - Raw files that have been uploaded to BQ are moved to <storage-bucket>/<region-code>/raw/<year>/<month>/<day>/
            subdirectory
        - If are_ingest_view_exports_enabled_in_env() is not True, we will create a copy of the 'raw' file with the
            'ingest_view' type in the name and save it to the ingest bucket once we're done processing the raw file (if
            the tag exists in the region's controller ingest tags).

        Conditions to enable for region:
        - is_raw_vs_ingest_file_name_detection_enabled() is already True for this environment w/ all preconditions met
        - Region has raw file yaml config with all expected raw files listed and all primary key / expected column
            configs completed

        If the |raw_data_bq_imports_enabled_env| config is unset, returns False. If it is set to 'prod',
        BQ import will also be enabled in staging.
        """
        return self.is_raw_vs_ingest_file_name_detection_enabled() and \
            self.raw_data_bq_imports_enabled_env is not None and \
            (not environment.in_gcp_production() or
             self.raw_data_bq_imports_enabled_env == environment.get_gcp_environment())

    def are_ingest_view_exports_enabled_in_env(self) -> bool:
        """Returns true if this regions supports export of ingest views to the ingest bucket.

        Side effects when enabled:
        - For this region, we will create a us_xx_ingest_views BQ dataset on launch to store raw data tables for that
            region (if it does not already exist).
        - Once all raw BQ pre-processing complete, we will export a diff of all updated ingest views based on
            information in the latest_valid_ingest_file_by_view table in BQ
        - When a view diff is exported, we will update the ingest_file_metadata table in BQ with information about the
            exported file.

        Conditions to enable for region:
        - are_raw_data_bq_imports_enabled_in_env() is already True for this environment w/ all preconditions met
        - Ingest views implemented in an ingest_views/ directory for all ingest file tags the controller expects to see

        If the |ingest_view_exports_enabled_env| config is unset, returns False. If it is set to 'prod',
        ingest view export will also be enabled in staging.
        """
        return self.is_raw_vs_ingest_file_name_detection_enabled() and \
            self.are_raw_data_bq_imports_enabled_in_env() and \
            self.ingest_view_exports_enabled_env is not None and \
            (not environment.in_gcp_production() or
             self.ingest_view_exports_enabled_env == environment.get_gcp_environment())


def get_region(region_code: str, is_direct_ingest: bool = False,
               region_module_override: Optional[ModuleType] = None) -> Region:
    global REGIONS

    if region_module_override:
        region_module = region_module_override
    elif is_direct_ingest:
        region_module = direct_ingest_regions_module
    else:
        region_module = scraper_regions_module

    ingest_type = IngestType.DIRECT_INGEST if is_direct_ingest else IngestType.SCRAPER
    if ingest_type not in REGIONS:
        REGIONS[ingest_type] = {}

    if region_code not in REGIONS[ingest_type]:
        REGIONS[ingest_type][region_code] = Region(region_code=region_code.lower(),
                                                   is_direct_ingest=is_direct_ingest,
                                                   region_module=region_module,
                                                   **get_region_manifest(region_code.lower(), region_module))
    return REGIONS[ingest_type][region_code]


MANIFEST_NAME = 'manifest.yaml'


def get_region_manifest(region_code: str,
                        region_module: ModuleType) -> Dict[str, Any]:
    """Gets manifest for a specific region

    Args:
        region_code: (string) Region code

    Returns:
        Region manifest as dictionary
    """
    with open(os.path.join(os.path.dirname(region_module.__file__),
                           region_code,
                           MANIFEST_NAME)) as region_manifest:
        return yaml.full_load(region_manifest)


def get_supported_scrape_region_codes(timezone: tzinfo = None,
                                      stripes: List[str] = None) -> Set[str]:
    """Retrieve a list of known scraper regions / region codes

    Args:
        timezone: (str) If set, return only regions in the right timezone
        stripes: (List[str]) If set, return only regions in the right stripes

    Returns:
        Set of region codes (strings)
    """
    return _get_supported_region_codes_for_base_region_module(
        scraper_regions_module,
        is_direct_ingest=False,
        timezone=timezone,
        stripes=stripes)


def get_supported_direct_ingest_region_codes() -> Set[str]:
    return _get_supported_region_codes_for_base_region_module(
        direct_ingest_regions_module,
        is_direct_ingest=True)


def _get_supported_region_codes_for_base_region_module(
        base_region_module: ModuleType,
        is_direct_ingest: bool,
        timezone: tzinfo = None,
        stripes: List[str] = None):

    base_region_path = os.path.dirname(base_region_module.__file__)
    all_region_codes = {region_module.name for region_module
                        in pkgutil.iter_modules([base_region_path])}
    if timezone:
        dt = datetime.now()
        filtered_regions = {region_code for region_code in all_region_codes
                            if timezone.utcoffset(dt) ==
                            get_region(
                                region_code,
                                is_direct_ingest=is_direct_ingest
                            ).timezone.utcoffset(dt)}
    else:
        filtered_regions = all_region_codes

    if stripes and len(stripes) > 0:
        return {region_code for region_code in filtered_regions
                if get_region(
                    region_code,
                    is_direct_ingest=is_direct_ingest
                ).stripe in stripes}

    return filtered_regions


def get_supported_regions() -> List['Region']:
    return get_supported_scrape_regions() + \
        get_supported_direct_ingest_regions()


def get_supported_scrape_regions() -> List['Region']:
    return [get_region(region_code, is_direct_ingest=False)
            for region_code in get_supported_scrape_region_codes()]


def get_supported_direct_ingest_regions() -> List['Region']:
    return [get_region(region_code, is_direct_ingest=True)
            for region_code in get_supported_direct_ingest_region_codes()]


def validate_region_code(region_code):
    """Verifies a region code is one of the Recidiviz supported regions.

    Args:
        region_code: (string) Region code (e.g., us_ny)

    Returns:
        True if valid region
        False if invalid region
    """
    return region_code in get_supported_scrape_region_codes()


def get_ingestor_name(region_code: str, ingest_type_name: str) -> str:
    """Returns the class name for a given region_code and ingest_module"""
    return ''.join(s.title() for s in
                   chain(region_code.split('_'), [ingest_type_name]))
