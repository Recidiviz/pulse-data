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

import { IngestRawFileProcessingStatus, SPECIAL_FILE_TAGS } from "../constants";

interface RenderRawDataFileTagProps {
  status: IngestRawFileProcessingStatus;
  storageDirectoryPath: string; // TODO(#15070) Add storage bucket link to file tag when datetimes_contained_upper_bound_inclusive available
}

const RawDataHasConfigFileCellContents: React.FC<RenderRawDataFileTagProps> = ({
  status,
}) => {
  const { fileTag, hasConfig } = status;
  const isSpecialTag = SPECIAL_FILE_TAGS.includes(fileTag);
  if (isSpecialTag) {
    return <div>{fileTag}</div>;
  }

  return (
    <div>
      {hasConfig ? (
        fileTag
      ) : (
        <>
          {fileTag} <em>(unrecognized)</em>
        </>
      )}
    </div>
  );
};

export default RawDataHasConfigFileCellContents;
