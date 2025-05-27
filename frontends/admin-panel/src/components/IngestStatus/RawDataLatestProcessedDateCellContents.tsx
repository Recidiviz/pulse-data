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

import NewTabLink from "../NewTabLink";
import {
  FILE_TAG_IGNORED_IN_SUBDIRECTORY,
  FILE_TAG_UNNORMALIZED,
  IngestRawFileProcessingStatus,
} from "./constants";
import { getGCPBucketURL } from "./ingestStatusUtils";

interface RawDataLatestDatetimeCellContentsProps {
  datetime: string | null;
  status: IngestRawFileProcessingStatus;
  storageDirectoryPath: string | undefined;
}

const RawDataLatestDatetimeCellContents: React.FC<
  RawDataLatestDatetimeCellContentsProps
> = ({ datetime, status, storageDirectoryPath }) => {
  const { fileTag, hasConfig, latestProcessedTime, latestUpdateDatetime } =
    status;

  if (fileTag === FILE_TAG_IGNORED_IN_SUBDIRECTORY) {
    return <div className="ingest-caution">N/A - Ignored</div>;
  }

  if (fileTag === FILE_TAG_UNNORMALIZED) {
    return <div className="ingest-danger">N/A - Unknown file tag</div>;
  }

  if (hasConfig && latestProcessedTime && latestUpdateDatetime) {
    const date = new Date(latestUpdateDatetime);
    return storageDirectoryPath ? (
      <NewTabLink
        href={getIngestStorageBucketPath(storageDirectoryPath, fileTag, date)}
      >
        {datetime}
      </NewTabLink>
    ) : (
      <div>{datetime}</div>
    );
  }

  return (
    <div>{hasConfig ? datetime : "N/A - No Raw Config File Available"}</div>
  );
};

export default RawDataLatestDatetimeCellContents;

function normalizeRelativePath(path: string): string {
  if (path[path.length - 1] !== "/") {
    return `${path}/`;
  }

  return path;
}

function getIngestStorageBucketPath(
  storageDirectoryPath: string,
  fileTag: string,
  date: Date
) {
  return getGCPBucketURL(
    `${normalizeRelativePath(
      storageDirectoryPath
    )}raw/${date.getUTCFullYear()}/${(date.getUTCMonth() + 1)
      .toString()
      .padStart(2, "0")}/${date.getUTCDate().toString().padStart(2, "0")}`,
    fileTag
  );
}
