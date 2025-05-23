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
import { Descriptions, Spin } from "antd";
import * as React from "react";

import { IngestRawFileProcessingStatus } from "./constants";

interface InstanceRawFileMetadataProps {
  stateCode: string;
  loading: boolean;
  ingestRawFileProcessingStatus: IngestRawFileProcessingStatus[];
}

// TODO(#39896) fix tooltips!
const InstanceRawFileMetadata: React.FC<InstanceRawFileMetadataProps> = ({
  stateCode,
  loading,
  ingestRawFileProcessingStatus: ingestFileProcessingStatus,
}) => {
  return (
    <div>
      <p>
        Information about the state of raw data processing in {stateCode}. Hover
        on each row title for more info.
      </p>
      <Descriptions bordered>
        <Descriptions.Item
          label={
            <div title="Number of raw data files registered in the operations database that are not yet marked as processed.">
              # Files Pending Import
            </div>
          }
          span={3}
        >
          {loading ? (
            <Spin />
          ) : (
            getNumberOfFilesPendingUpload(ingestFileProcessingStatus)
          )}
        </Descriptions.Item>
        <Descriptions.Item
          label={
            <div title="Number of raw data files found in ingest bucket that don't have configuration yaml.">
              # Unrecognized Files
            </div>
          }
          span={3}
        >
          {loading ? (
            <Spin />
          ) : (
            getNumberOfUnrecognizedFiles(ingestFileProcessingStatus)
          )}
        </Descriptions.Item>
      </Descriptions>
    </div>
  );
};

function getNumberOfFilesPendingUpload(
  ingestRawFileProcessingStatus: IngestRawFileProcessingStatus[]
): number {
  return ingestRawFileProcessingStatus
    .map((x) => x.numberUnprocessedFiles)
    .reduce((a, b) => a + b, 0);
}

function getNumberOfUnrecognizedFiles(
  ingestRawFileProcessingStatus: IngestRawFileProcessingStatus[]
): number {
  return ingestRawFileProcessingStatus
    .filter((x) => !x.hasConfig)
    .map((x) => x.numberFilesInBucket)
    .reduce((a, b) => a + b, 0);
}

export default InstanceRawFileMetadata;
