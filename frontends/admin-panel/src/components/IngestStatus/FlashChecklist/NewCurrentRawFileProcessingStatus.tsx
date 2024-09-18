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
import { IngestRawFileProcessingStatus } from "../constants";

export class NewCurrentRawFileProcessingStatus {
  primary: IngestRawFileProcessingStatus[] | undefined;

  secondary: IngestRawFileProcessingStatus[] | undefined;

  unprocessedFilesInPrimary: IngestRawFileProcessingStatus[];

  unprocessedFilesInSecondary: IngestRawFileProcessingStatus[];

  emptyIngestBuckets: boolean;

  constructor({
    primary,
    secondary,
  }: {
    primary: IngestRawFileProcessingStatus[] | undefined;
    secondary: IngestRawFileProcessingStatus[] | undefined;
  }) {
    this.primary = primary;
    this.secondary = secondary;
    this.unprocessedFilesInPrimary =
      this.primary !== undefined
        ? this.primary.filter((info) => info.numberFilesInBucket !== 0)
        : [];
    this.unprocessedFilesInSecondary =
      this.secondary !== undefined
        ? this.secondary.filter((info) => info.numberFilesInBucket !== 0)
        : [];
    this.emptyIngestBuckets =
      this.unprocessedFilesInPrimary.length === 0 &&
      this.unprocessedFilesInSecondary.length === 0;
  }

  hasProcessedFilesInSecondary() {
    if (!this.secondary) return false;

    return this.secondary.reduce(
      (acc, status) =>
        acc || (status.hasConfig && status.numberProcessedFiles > 0),
      false
    );
  }

  hasUnprocessedFilesInSecondary() {
    if (!this.secondary) return false;

    return this.secondary.reduce(
      (acc, status) => acc || status.numberUnprocessedFiles > 0,
      this.unprocessedFilesInSecondary.length !== 0
    );
  }
}
