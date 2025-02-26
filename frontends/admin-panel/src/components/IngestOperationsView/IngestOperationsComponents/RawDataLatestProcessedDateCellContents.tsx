// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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

import NewTabLink from "../../NewTabLink";
import {
  FILE_TAG_IGNORED_IN_SUBDIRECTORY,
  FILE_TAG_UNNORMALIZED,
  IngestRawFileProcessingStatus,
} from "../constants";
import { getGCPBucketURL } from "../ingestStatusUtils";

interface RawDataLatestProcessedDateCellContentsProps {
  status: IngestRawFileProcessingStatus;
  storageDirectoryPath: string;
}

const RawDataLatestProcessedDateCellContents: React.FC<RawDataLatestProcessedDateCellContentsProps> =
  ({ status, storageDirectoryPath }) => {
    const {
      fileTag,
      hasConfig,
      latestProcessedTime,
      latestProcessedDatetimeContainedUpperBoundInclusive,
    } = status;

    if (fileTag === FILE_TAG_IGNORED_IN_SUBDIRECTORY) {
      return <div className="ingest-caution">N/A - Ignored</div>;
    }

    if (fileTag === FILE_TAG_UNNORMALIZED) {
      <div className="ingest-danger">N/A - Unknown file tag</div>;
    }

    if (
      hasConfig &&
      latestProcessedTime &&
      latestProcessedDatetimeContainedUpperBoundInclusive
    ) {
      const date = new Date(
        latestProcessedDatetimeContainedUpperBoundInclusive
      );
      return (
        <NewTabLink
          href={getIngestStorageBucketPath(storageDirectoryPath, fileTag, date)}
        >
          {latestProcessedTime}
        </NewTabLink>
      );
    }

    return (
      <div>
        {hasConfig ? latestProcessedTime : "N/A - No Raw Config File Available"}
      </div>
    );
  };

export default RawDataLatestProcessedDateCellContents;

function getIngestStorageBucketPath(
  storageDirectoryPath: string,
  fileTag: string,
  date: Date
) {
  return getGCPBucketURL(
    `${storageDirectoryPath}/raw/${date.getUTCFullYear()}/${(
      date.getUTCMonth() + 1
    )
      .toString()
      .padStart(2, "0")}/${date.getUTCDate().toString().padStart(2, "0")}`,
    fileTag
  );
}
