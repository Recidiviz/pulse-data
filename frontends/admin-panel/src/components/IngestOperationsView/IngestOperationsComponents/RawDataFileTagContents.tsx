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

import classNames from "classnames";
import NewTabLink from "../../NewTabLink";
import { IngestRawFileProcessingStatus, SPECIAL_FILE_TAGS } from "../constants";
import { getGCPBucketURL } from "../ingestStatusUtils";

interface RenderHasConfigFileProps {
  status: IngestRawFileProcessingStatus;
  ingestBucketPath: string;
}

const RawDataFileTagContents: React.FC<RenderHasConfigFileProps> = ({
  status,
  ingestBucketPath,
}) => {
  const { fileTag, hasConfig } = status;
  const isSpecialTag = SPECIAL_FILE_TAGS.includes(fileTag);
  if (isSpecialTag) {
    return <div>N/A</div>;
  }
  return (
    <div
      className={classNames({
        "ingest-caution": !hasConfig,
      })}
    >
      {hasConfig ? (
        "Yes"
      ) : (
        <NewTabLink href={getGCPBucketURL(ingestBucketPath, fileTag)}>
          No Raw Config File Available
        </NewTabLink>
      )}
    </div>
  );
};

export default RawDataFileTagContents;
