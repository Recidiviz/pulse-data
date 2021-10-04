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

import { Card, Descriptions, Empty, Table } from "antd";
import { ColumnsType } from "antd/lib/table";
import React from "react";
import { SamenessPerViewValidationResultDetails } from "../../recidiviz/admin_panel/models/validation_pb";
import { SamenessPerViewDetailsProps } from "./constants";

// =============================================================================
const SamenessPerViewDetails: React.FC<SamenessPerViewDetailsProps> = ({
  samenessPerView,
}) => {
  if (samenessPerView === undefined) {
    return <Empty />;
  }

  const sortedPartitions = samenessPerView
    .getNonNullCountsPerColumnPerPartitionList()
    .sort((a, b) => {
      const aLabels = a.getPartitionLabelsList();
      const bLabels = b.getPartitionLabelsList();
      for (let i = 0; i < Math.min(aLabels.length, bLabels.length); i += 1) {
        if (aLabels[i] > bLabels[i]) {
          return 1;
        }
        if (aLabels[i] < bLabels[i]) {
          return -1;
        }
      }
      return 0;
    });
  const firstPartition = sortedPartitions[0];
  const partitionColumns: ColumnsType<SamenessPerViewValidationResultDetails.PartitionCounts> =
    firstPartition
      .getPartitionLabelsList()
      .map((value: string, index: number) => {
        return {
          key: `partition-${index}`,
          fixed: "left",
          render: (
            _: string,
            item: SamenessPerViewValidationResultDetails.PartitionCounts
          ) => item.getPartitionLabelsList()[index],
        };
      });
  const columns = partitionColumns.concat(
    firstPartition
      .getColumnCountsMap()
      .getEntryList()
      .sort((a: [string, number], b: [string, number]) =>
        a[0] > b[0] ? 1 : -1
      )
      .map((value: [string, number]) => {
        // TODO(#9481): Add error rate per partition
        return {
          title: value[0],
          key: value[0],
          render: (
            _: string,
            item: SamenessPerViewValidationResultDetails.PartitionCounts
          ) => item.getColumnCountsMap().get(value[0]),
        };
      })
  );

  return (
    <>
      <Card title="Error Details">
        <Descriptions bordered>
          <Descriptions.Item label="Error Rows">
            {samenessPerView.getNumErrorRows()}
          </Descriptions.Item>
          <Descriptions.Item label="Total Rows">
            {samenessPerView.getTotalNumRows()}
          </Descriptions.Item>
        </Descriptions>
      </Card>
      <Card title="Partition Counts">
        <Table
          className="validation-table"
          columns={columns}
          dataSource={sortedPartitions}
          pagination={{
            hideOnSinglePage: true,
            showSizeChanger: true,
            defaultPageSize: 5,
            size: "small",
          }}
        />
      </Card>
    </>
  );
};

export default SamenessPerViewDetails;
