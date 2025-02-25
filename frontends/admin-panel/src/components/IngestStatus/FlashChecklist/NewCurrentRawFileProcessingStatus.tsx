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
import { ConditionalColoredSquare } from "./FlashComponents";

export class NewCurrentRawFileProcessingStatus {
  primary: IngestRawFileProcessingStatus[] | undefined;

  secondary: IngestRawFileProcessingStatus[] | undefined;

  filesInBucketInPrimary: IngestRawFileProcessingStatus[];

  filesInBucketInSecondary: IngestRawFileProcessingStatus[];

  constructor({
    primary,
    secondary,
  }: {
    primary: IngestRawFileProcessingStatus[] | undefined;
    secondary: IngestRawFileProcessingStatus[] | undefined;
  }) {
    this.primary = primary;
    this.secondary = secondary;
    this.filesInBucketInPrimary =
      this.primary !== undefined
        ? this.primary.filter((info) => info.numberFilesInBucket !== 0)
        : [];
    this.filesInBucketInSecondary =
      this.secondary !== undefined
        ? this.secondary.filter((info) => info.numberFilesInBucket !== 0)
        : [];
  }

  ingestBucketSummary(projectId: string, formattedStateCode: string) {
    const primaryBucketURL = `https://console.cloud.google.com/storage/browser/${projectId}-direct-ingest-state-${formattedStateCode}`;
    const secondaryBucketURL = `https://console.cloud.google.com/storage/browser/${projectId}-direct-ingest-state-${formattedStateCode}-secondary`;
    return (
      <div>
        <em>
          <a href={primaryBucketURL}>PRIMARY INGEST BUCKET</a> :{" "}
        </em>
        <ConditionalColoredSquare
          boolVal={this.filesInBucketInPrimary.length === 0}
        />
        {this.filesInBucketInPrimary.length !== 0 && (
          <ul>
            {this.filesInBucketInPrimary.map((o) => (
              <li>
                {o.fileTag}: {o.numberFilesInBucket}
              </li>
            ))}
          </ul>
        )}
        <br />
        <em>
          <a href={secondaryBucketURL}>SECONDARY INGEST BUCKET</a> :{" "}
          <ConditionalColoredSquare
            boolVal={this.filesInBucketInSecondary.length === 0}
          />
        </em>
        {this.filesInBucketInSecondary.length !== 0 && (
          <ul>
            {this.filesInBucketInSecondary.map((o) => (
              <li>
                {o.fileTag}: {o.numberFilesInBucket}
              </li>
            ))}
          </ul>
        )}
      </div>
    );
  }

  areIngestBucketsEmpty() {
    return (
      this.filesInBucketInPrimary.length === 0 &&
      this.filesInBucketInSecondary.length === 0
    );
  }

  hasProcessedFilesInSecondary() {
    if (!this.secondary) return false;

    return this.secondary.reduce(
      (acc, status) =>
        acc || (status.hasConfig && status.numberProcessedFiles > 0),
      false
    );
  }

  unprocessedFilesInSecondary() {
    if (!this.secondary) return [];

    return this.secondary.filter(
      (info) => info.numberUnprocessedFiles > 0 || info.numberUngroupedFiles > 0
    );
  }

  hasUnprocessedFilesInSecondary() {
    if (!this.secondary) return false;

    return this.unprocessedFilesInSecondary().length > 0;
  }

  unprocessedSecondaryDescription() {
    return (
      <div>
        We should not flash until our re-import has completed{" "}
        {this.hasUnprocessedFilesInSecondary() && (
          <>
            Unprocessed files:
            <ul>
              {this.unprocessedFilesInSecondary().map((item) => (
                <li>
                  {item.fileTag}: {item.numberUnprocessedFiles}
                </li>
              ))}
            </ul>
          </>
        )}
      </div>
    );
  }
}
