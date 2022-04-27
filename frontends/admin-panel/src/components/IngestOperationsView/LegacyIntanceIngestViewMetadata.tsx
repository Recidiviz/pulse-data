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
import { Col, Descriptions, Row, Statistic } from "antd";
import * as React from "react";
import { IngestInstanceSummary } from "./constants";

interface LegacyInstanceIngestViewMetadataProps {
  data: IngestInstanceSummary;
}

// TODO(#11424): Delete this element / whole file once we have migrated all states to BQ
// ingest view materialization.
const LegacyInstanceIngestViewMetadata: React.FC<LegacyInstanceIngestViewMetadataProps> =
  ({ data }) => {
    return (
      <div>
        <p>
          The counts from each section below should be the same. If they are
          not, sort of cleanup operation was botched and we should investigate
          the differences.
        </p>
        <Descriptions bordered>
          <Descriptions.Item label="GCS file metadata" span={3}>
            <Row gutter={16}>
              <Col span={12}>
                <Statistic
                  title="Unprocessed"
                  value={data.ingest.unprocessedFilesIngestView}
                />
              </Col>
              <Col span={12}>
                <Statistic
                  title="Processed"
                  value={data.ingest.processedFilesIngestView}
                />
              </Col>
            </Row>
          </Descriptions.Item>
          <Descriptions.Item label="Operations DB file metadata" span={3}>
            <Row gutter={16}>
              <Col span={12}>
                <Statistic
                  title="Unprocessed"
                  value={data.operations.unprocessedFilesIngestView}
                />
              </Col>
              <Col span={12}>
                <Statistic
                  title="Processed"
                  value={data.operations.processedFilesIngestView}
                />
              </Col>
            </Row>
          </Descriptions.Item>
          <Descriptions.Item
            label="Date of Earliest Unprocessed Ingest File"
            span={3}
          >
            {data.operations.dateOfEarliestUnprocessedIngestView}
          </Descriptions.Item>
        </Descriptions>
      </div>
    );
  };

export default LegacyInstanceIngestViewMetadata;
