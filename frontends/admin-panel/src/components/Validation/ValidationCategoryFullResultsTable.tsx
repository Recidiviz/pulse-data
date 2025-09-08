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

import { Table } from "antd";
import { ColumnsType, ColumnType } from "antd/lib/table";
import { FC } from "react";
import { useHistory } from "react-router-dom";

import { ValidationStatusRecord } from "../../recidiviz/admin_panel/models/validation_pb";
import { MetadataRecord } from "../../types";
import {
  getUniqueValues,
  optionalNumberSort,
} from "../Utilities/GeneralUtilities";
import { RecordStatus } from "./constants";
import RenderRecordStatus from "./RenderRecordStatus";
import { getRecordStatus, handleClickToDetails } from "./utils";

interface ValidationCategoryFullResultsTableProps {
  categoryId: string;
  dictOfCategoryIdsToRecords: {
    [category: string]: MetadataRecord<ValidationStatusRecord>[];
  };
  selectedStates: string[];
  loading?: boolean;
}

const ValidationCategoryFullResultsTable: FC<
  ValidationCategoryFullResultsTableProps
> = ({ categoryId, loading, dictOfCategoryIdsToRecords, selectedStates }) => {
  const history = useHistory();

  function columnTypeForState(
    state: string
  ): ColumnType<MetadataRecord<ValidationStatusRecord>> {
    return {
      title: selectedStates.length > 1 ? state : "Status",
      key: state,
      onCell: (record) => {
        return {
          onClick: handleClickToDetails(history, record.name, state),
        };
      },
      render: (_: string, record: MetadataRecord<ValidationStatusRecord>) => (
        <RenderRecordStatus record={record.resultsByState[state]} />
      ),
      sorter: (a, b) =>
        optionalNumberSort(
          a.resultsByState[state]?.getErrorAmount(),
          b.resultsByState[state]?.getErrorAmount()
        ),
      filters: getUniqueValues(
        dictOfCategoryIdsToRecords[categoryId].map((record) =>
          getRecordStatus(record.resultsByState[state])
        )
      ).map((status) => ({
        text: RecordStatus[status],
        value: status,
      })),
      onFilter: (value, record) =>
        getRecordStatus(record.resultsByState[state]) === value,
    };
  }
  function getLabelColumns(): ColumnsType<
    MetadataRecord<ValidationStatusRecord>
  > {
    const labelColumns: ColumnsType<MetadataRecord<ValidationStatusRecord>> = [
      {
        title: "Validation Name",
        key: "validation",
        fixed: "left",
        width: "55%",
        onCell: (record) => {
          return {
            onClick: handleClickToDetails(
              history,
              record.name,
              selectedStates.length === 1 ? selectedStates[0] : undefined
            ),
          };
        },
        render: (_: string, record: MetadataRecord<ValidationStatusRecord>) => (
          <div>{record.name}</div>
        ),
        sorter: (a, b) => a.name.localeCompare(b.name),
        defaultSortOrder: "ascend",
        filters: dictOfCategoryIdsToRecords[categoryId].map(({ name }) => ({
          text: name,
          value: name,
        })),
        onFilter: (value, content) => content.name === value,
        filterSearch: true,
      },
    ];

    return labelColumns.concat(
      selectedStates.map((s) => columnTypeForState(s))
    );
  }

  return (
    <Table
      className="validation-table"
      columns={getLabelColumns()}
      loading={loading}
      dataSource={dictOfCategoryIdsToRecords[categoryId]}
      pagination={{
        hideOnSinglePage: true,
        showSizeChanger: true,
        pageSize: 50,
        size: "small",
      }}
      rowClassName="validation-table-row"
      rowKey={(record) => record.name}
    />
  );
};

export default ValidationCategoryFullResultsTable;
