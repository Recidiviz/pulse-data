// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { IngestStatus } from "../constants";

export class LegacyCurrentRawDataInstanceStatus {
  primary: IngestStatus | undefined;

  secondary: IngestStatus | undefined;

  isFlashInProgress() {
    return (
      this.primary === IngestStatus.FLASH_IN_PROGRESS &&
      this.secondary === IngestStatus.FLASH_IN_PROGRESS
    );
  }

  isReimportCancellationInProgress() {
    return (
      this.secondary === IngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS
    );
  }

  isReimportCanceled() {
    return this.secondary === IngestStatus.RAW_DATA_REIMPORT_CANCELED;
  }

  isReadyToFlash() {
    return this.secondary === IngestStatus.READY_TO_FLASH;
  }

  isFlashCompleted() {
    return this.secondary === IngestStatus.FLASH_COMPLETED;
  }

  isNoReimportInProgress() {
    return this.secondary === IngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS;
  }

  constructor({
    primary,
    secondary,
  }: {
    primary: string | undefined;
    secondary: string | undefined;
  }) {
    this.primary = primary as IngestStatus;
    this.secondary = secondary as IngestStatus;
  }
}
