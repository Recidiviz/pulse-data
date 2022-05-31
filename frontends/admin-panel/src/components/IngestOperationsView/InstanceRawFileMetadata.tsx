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
import { Descriptions } from "antd";
import * as React from "react";
import { DirectIngestInstance, OperationsDbInfo } from "./constants";

interface InstanceRawFileMetadataProps {
  stateCode: string;
  instance: DirectIngestInstance;
  operationsInfo: OperationsDbInfo;
  numFilesIngestBucket: number;
}

const InstanceRawFileMetadata: React.FC<InstanceRawFileMetadataProps> = ({
  stateCode,
  instance,
  operationsInfo,
  numFilesIngestBucket,
}) => {
  // TODO(#12387): Update this to change if the current secondary rerun is using raw data in secondary.
  const isSecondaryUsingPrimary = instance === DirectIngestInstance.SECONDARY;

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
              Unprocessed files
            </div>
          }
          span={3}
        >
          {isSecondaryUsingPrimary
            ? "N/A - ingest in SECONDARY using PRIMARY raw data"
            : operationsInfo.unprocessedFilesRaw}
        </Descriptions.Item>
        <Descriptions.Item
          label={
            <div title="Number of raw data files registered in the operations database that have been marked as processed.">
              Processed files
            </div>
          }
          span={3}
        >
          {isSecondaryUsingPrimary
            ? "N/A - ingest in SECONDARY using PRIMARY raw data"
            : operationsInfo.processedFilesRaw}
        </Descriptions.Item>
        <Descriptions.Item
          label={
            <div title="Number of raw data files of any kind in the ingest bucket. This should match the number of unprocessed files.">
              Files in ingest bucket
            </div>
          }
          span={3}
        >
          {numFilesIngestBucket}
        </Descriptions.Item>
      </Descriptions>
    </div>
  );
};

export default InstanceRawFileMetadata;
