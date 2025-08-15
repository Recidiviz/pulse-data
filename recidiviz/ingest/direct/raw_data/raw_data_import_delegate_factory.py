# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Contains factory class for creating BaseRawDataImportDelegate objects"""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.base_raw_data_import_delegate import (
    BaseRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_ar.us_ar_raw_data_import_delegate import (
    UsArRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_az.us_az_raw_data_import_delegate import (
    UsAzRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_ca.us_ca_raw_data_import_delegate import (
    UsCaRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_co.us_co_raw_data_import_delegate import (
    UsCoRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_ia.us_ia_raw_data_import_delegate import (
    UsIaRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_id.us_id_raw_data_import_delegate import (
    UsIdRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_ix.us_ix_raw_data_import_delegate import (
    UsIxRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_ma.us_ma_raw_data_import_delegate import (
    UsMaRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_me.us_me_raw_data_import_delegate import (
    UsMeRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_mi.us_mi_raw_data_import_delegate import (
    UsMiRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_mo.us_mo_raw_data_import_delegate import (
    UsMoRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_nc.us_nc_raw_data_import_delegate import (
    UsNcRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_raw_data_import_delegate import (
    UsNdRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_ne.us_ne_raw_data_import_delegate import (
    UsNeRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_ny.us_ny_raw_data_import_delegate import (
    UsNyRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_or.us_or_raw_data_import_delegate import (
    UsOrRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_oz.us_oz_raw_data_import_delegate import (
    UsOzRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_pa.us_pa_raw_data_import_delegate import (
    UsPaRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_tn.us_tn_raw_data_import_delegate import (
    UsTnRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_tx.us_tx_raw_data_import_delegate import (
    UsTxRawDataImportDelegate,
)
from recidiviz.ingest.direct.regions.us_ut.us_ut_raw_data_import_delegate import (
    UsUtRawDataImportDelegate,
)


class RawDataImportDelegateFactory:
    """Factory class for creating BaseRawDataImportDelegate objects"""

    @classmethod
    def build(cls, *, region_code: str) -> BaseRawDataImportDelegate:
        """Builds a BaseRawDataImportDelegate class for the provided |region_code|."""
        region_code = region_code.upper()
        if region_code == StateCode.US_AR.value:
            return UsArRawDataImportDelegate()
        if region_code == StateCode.US_AZ.value:
            return UsAzRawDataImportDelegate()
        if region_code == StateCode.US_CA.value:
            return UsCaRawDataImportDelegate()
        if region_code == StateCode.US_CO.value:
            return UsCoRawDataImportDelegate()
        if region_code == StateCode.US_IA.value:
            return UsIaRawDataImportDelegate()
        if region_code == StateCode.US_ID.value:
            return UsIdRawDataImportDelegate()
        if region_code == StateCode.US_IX.value:
            return UsIxRawDataImportDelegate()
        if region_code == StateCode.US_MA.value:
            return UsMaRawDataImportDelegate()
        if region_code == StateCode.US_ME.value:
            return UsMeRawDataImportDelegate()
        if region_code == StateCode.US_MI.value:
            return UsMiRawDataImportDelegate()
        if region_code == StateCode.US_MO.value:
            return UsMoRawDataImportDelegate()
        if region_code == StateCode.US_NC.value:
            return UsNcRawDataImportDelegate()
        if region_code == StateCode.US_ND.value:
            return UsNdRawDataImportDelegate()
        if region_code == StateCode.US_NE.value:
            return UsNeRawDataImportDelegate()
        if region_code == StateCode.US_NY.value:
            return UsNyRawDataImportDelegate()
        if region_code == StateCode.US_OR.value:
            return UsOrRawDataImportDelegate()
        if region_code == StateCode.US_OZ.value:
            return UsOzRawDataImportDelegate()
        if region_code == StateCode.US_PA.value:
            return UsPaRawDataImportDelegate()
        if region_code == StateCode.US_TN.value:
            return UsTnRawDataImportDelegate()
        if region_code == StateCode.US_TX.value:
            return UsTxRawDataImportDelegate()
        if region_code == StateCode.US_UT.value:
            return UsUtRawDataImportDelegate()
        raise ValueError(f"Unexpected region code provided: [{region_code}]")
