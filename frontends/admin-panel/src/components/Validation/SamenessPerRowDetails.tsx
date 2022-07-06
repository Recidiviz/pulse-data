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

import { Card, Empty, Table } from "antd";
import { ColumnsType } from "antd/lib/table";
import * as React from "react";
import { SamenessPerRowValidationResultDetails } from "../../recidiviz/admin_panel/models/validation_pb";
import { SamenessPerRowDetailsProps } from "./constants";
import { formatStatusAmount } from "./utils";

const SamenessPerRowDetails: React.FC<SamenessPerRowDetailsProps> = ({
  samenessPerRow,
}) => {
  if (samenessPerRow === undefined) {
    return <Empty />;
  }

  const sortedRows = samenessPerRow.getFailedRowsList().sort((a, b) => {
    const aLabels = a.getRow()?.getLabelValuesList();
    const bLabels = b.getRow()?.getLabelValuesList();
    if (aLabels === undefined) {
      return 1;
    }
    if (bLabels === undefined) {
      return -1;
    }
    for (let i = 0; i < Math.min(aLabels?.length, bLabels.length); i += 1) {
      if (aLabels[i] > bLabels[i]) {
        return 1;
      }
      if (aLabels[i] < bLabels[i]) {
        return -1;
      }
    }
    return 0;
  });

  if (!sortedRows.length) {
    return <></>;
  }

  const firstRow = sortedRows[0];
  const columns: ColumnsType<SamenessPerRowValidationResultDetails.RowWithError> =
    [
      {
        title: "Labels",
        children: firstRow
          .getRow()
          ?.getLabelValuesList()
          .map((value: string, index: number) => ({
            key: `label-${index}`,
            fixed: "left",
            render: (
              _: string,
              item: SamenessPerRowValidationResultDetails.RowWithError
            ) => item.getRow()?.getLabelValuesList()[index],
          })),
      },
      {
        title: "Values",
        children: firstRow
          .getRow()
          ?.getComparisonValuesList()
          .map((__, index: number) => ({
            key: `value-${index}`,
            render: (
              _,
              item: SamenessPerRowValidationResultDetails.RowWithError
            ) => item.getRow()?.getComparisonValuesList()[index].getValue(),
          })),
      },
      {
        title: "Error",
        key: "error",
        render: (
          _: string,
          item: SamenessPerRowValidationResultDetails.RowWithError
        ) => formatStatusAmount(item.getError(), true),
      },
    ];

  return (
    <Card title="Failed Rows">
      <Table
        className="validation-table"
        columns={columns}
        dataSource={sortedRows}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          defaultPageSize: 5,
          size: "small",
        }}
        bordered
      />
    </Card>
  );
};

export default SamenessPerRowDetails;
