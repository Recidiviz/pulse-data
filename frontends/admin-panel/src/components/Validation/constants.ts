// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================

import {
  SamenessPerRowValidationResultDetails,
  SamenessPerViewValidationResultDetails,
  ValidationStatusRecord,
} from "../../recidiviz/admin_panel/models/validation_pb";
import { IngestInstanceStatusInfo } from "../IngestStatus/constants";

export const ANCHOR_VALIDATION_FAILURE_SUMMARY = "failure-summary";
export const ANCHOR_VALIDATION_HARD_FAILURES = "hard-failures";
export const ANCHOR_VALIDATION_SOFT_FAILURES = "soft-failures";
export const ANCHOR_VALIDATION_FULL_RESULTS = "full-results";

export enum RecordStatus {
  NO_RESULT,
  BROKEN,
  NEED_DATA,
  // Any of these should have a result.
  FAIL_HARD,
  FAIL_SOFT,
  SUCCESS,
  UNKNOWN,
}

export interface ValidationDetailsProps {
  validationName: string;
  stateCode: string;
}

export interface ValidationDetailsGraphProps {
  records: ValidationStatusRecord[];
  isPercent: boolean | undefined;
  loading: boolean;
  ingestEvents: {
    refreshTimestamp: string;
    ingestStatuses: IngestInstanceStatusInfo[];
  }[];
  versionChanges: { [systemVersion: string]: string };
}

export interface SamenessPerRowDetailsProps {
  samenessPerRow: SamenessPerRowValidationResultDetails | undefined;
}

export interface SamenessPerViewDetailsProps {
  samenessPerView: SamenessPerViewValidationResultDetails | undefined;
}

export interface ValidationErrorTableData {
  metadata: ValidationErrorTableMetadata;
  rows: ValidationErrorTableRows[];
}

export interface ValidationErrorTableMetadata {
  query: string;
  limitedRowsShown: boolean;
  totalRows: number;
}

export interface ValidationErrorTableRows {
  [column: string]: string | number | Date;
}

export interface ValidationErrorTableProps {
  tableData: ValidationErrorTableData;
}

/** Information about a particular BigQuery refresh. */
export type IngestStatusRefreshInfo = {
  /** The timestamp that the BigQuery refresh occurred. */
  refreshTimestamp: string;
  /** All of the ingest statuses that were active during the time covered by this
   * refresh (at any point between the prior refresh and this refresh).
   *
   * For instance, if ingest view materialization was ongoing during the prior refresh,
   * and then extract and merge ran and completed and the ingest instance was up to date,
   * and this BigQuery refresh took place while it was up to date, all three of those
   * statuses will be present in this list. */
  ingestStatuses: IngestInstanceStatusInfo[];
};
