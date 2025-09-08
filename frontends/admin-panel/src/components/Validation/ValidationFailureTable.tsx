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

import { InfoCircleOutlined } from "@ant-design/icons";
import { Table, Tooltip } from "antd";
import { ColumnsType } from "antd/lib/table";
import { FC } from "react";
import { useHistory } from "react-router-dom";

import { ValidationStatusRecord } from "../../recidiviz/admin_panel/models/validation_pb";
import {
  getUniqueValues,
  optionalNumberSort,
  optionalStringSort,
} from "../Utilities/GeneralUtilities";
import { RecordStatus } from "./constants";
import RenderRecordStatus from "./RenderRecordStatus";
import {
  chooseIdNameForCategory,
  formatStatusAmount,
  getDaysActive,
  getRecordStatus,
  handleClickToDetails,
  readableNameForCategoryId,
  replaceInfinity,
} from "./utils";

interface ValdiationFailureTableProps {
  statuses: RecordStatus[];
  records: ValidationStatusRecord[];
  allStates: string[];
  selectedState: string | null;
  categoryIds: string[];
  loading?: boolean;
}

const ValidationFailureTable: FC<ValdiationFailureTableProps> = ({
  statuses,
  records,
  allStates,
  selectedState,
  categoryIds,
  loading,
}) => {
  const history = useHistory();

  function getFailureLabelColumns(): ColumnsType<ValidationStatusRecord> {
    return [
      {
        title: "Category",
        key: "category",
        fixed: "left",
        render: (_: string, record: ValidationStatusRecord) => (
          <div>
            {readableNameForCategoryId(
              chooseIdNameForCategory(record.getCategory())
            )}
          </div>
        ),
        sorter: (a: ValidationStatusRecord, b: ValidationStatusRecord) =>
          chooseIdNameForCategory(a.getCategory()).localeCompare(
            chooseIdNameForCategory(b.getCategory())
          ),
        filters: categoryIds.map((categoryId: string) => ({
          text: readableNameForCategoryId(categoryId),
          value: categoryId,
        })),
        onFilter: (value, record: ValidationStatusRecord) =>
          chooseIdNameForCategory(record.getCategory()) === value,
      },
      {
        title: "Validation Name",
        key: "validation",
        fixed: "left",
        width: "35%",
        render: (_: string, record: ValidationStatusRecord) => (
          <div>{record.getName()}</div>
        ),
        sorter: (a: ValidationStatusRecord, b: ValidationStatusRecord) =>
          optionalStringSort(a.getName(), b.getName()),
        filters: getListOfUniqueRecordNames(statuses, records).map((name) => ({
          text: name,
          value: name,
        })),
        onFilter: (value, content) => content.getName() === value,
        filterSearch: true,
      },
      {
        title: "State",
        key: "state",
        fixed: "left",
        render: (_: string, record: ValidationStatusRecord) => (
          <div>{record.getStateCode()}</div>
        ),
        sorter: (a: ValidationStatusRecord, b: ValidationStatusRecord) =>
          optionalStringSort(a.getStateCode(), b.getStateCode()),
        defaultSortOrder: "ascend",
        filters: selectedState
          ? undefined
          : allStates.map((state: string) => ({
              text: state,
              value: state,
            })),
        filteredValue: selectedState ? [selectedState] : undefined,
        onFilter: (value, record: ValidationStatusRecord) =>
          record.getStateCode() === value,
      },
      {
        title: "Status",
        key: "status",
        fixed: "left",
        render: (_: string, record: ValidationStatusRecord) => (
          <RenderRecordStatus record={record} />
        ),
        sorter: (a: ValidationStatusRecord, b: ValidationStatusRecord) =>
          optionalNumberSort(a.getErrorAmount(), b.getErrorAmount()),
        filters: statuses.map((status: RecordStatus) => ({
          text: RecordStatus[status],
          value: status,
        })),
        onFilter: (value, record: ValidationStatusRecord) =>
          getRecordStatus(record) === value,
      },
      {
        title: "Soft Threshold",
        key: "soft-failure-thresholds",
        fixed: "left",
        render: (_: string, record: ValidationStatusRecord) => (
          <div>
            {formatStatusAmount(
              record.getSoftFailureAmount(),
              record.getIsPercentage()
            )}
          </div>
        ),
        sorter: (a: ValidationStatusRecord, b: ValidationStatusRecord) =>
          optionalNumberSort(
            a.getSoftFailureAmount(),
            b.getSoftFailureAmount()
          ),
      },
      {
        title: "Hard Threshold",
        key: "hard-failure-thresholds",
        fixed: "left",
        render: (_: string, record: ValidationStatusRecord) => (
          <div>
            {formatStatusAmount(
              record.getHardFailureAmount(),
              record.getIsPercentage()
            )}
          </div>
        ),
        sorter: (a: ValidationStatusRecord, b: ValidationStatusRecord) =>
          optionalNumberSort(
            a.getHardFailureAmount(),
            b.getHardFailureAmount()
          ),
      },
      {
        title: (
          <span>
            Days Active&nbsp;
            <Tooltip title="Number of days the validation has been failing with this status or worse.">
              <InfoCircleOutlined />
            </Tooltip>
          </span>
        ),
        key: "days-active",
        fixed: "left",
        render: (_: string, record: ValidationStatusRecord) => (
          <div>{replaceInfinity(getDaysActive(record), "Always")}</div>
        ),
        sorter: (a: ValidationStatusRecord, b: ValidationStatusRecord) =>
          getDaysActive(a) - getDaysActive(b),
        showSorterTooltip: false,
      },
    ];
  }

  return (
    <Table
      className="validation-table"
      columns={getFailureLabelColumns()}
      onRow={(record) => {
        return {
          onClick: handleClickToDetails(
            history,
            record.getName(),
            record.getStateCode()
          ),
        };
      }}
      loading={loading}
      dataSource={getListOfFilteredRecords(statuses, records)}
      pagination={{
        hideOnSinglePage: true,
        showSizeChanger: true,
        pageSize: 50,
        size: "small",
      }}
      rowClassName="validation-table-row"
      rowKey={(record) => `${record.getName()}-${record.getStateCode()}`}
    />
  );
};

export default ValidationFailureTable;

const getListOfFilteredRecords = (
  filterTypes: RecordStatus[],
  records: ValidationStatusRecord[]
): ValidationStatusRecord[] => {
  return records
    .filter(
      (record: ValidationStatusRecord) =>
        !record?.getDevMode() && filterTypes.includes(getRecordStatus(record))
    )
    .sort((a, b) => {
      if (a.getName() === b.getName()) {
        return (a.getStateCode() || "") > (b.getStateCode() || "") ? 1 : -1;
      }
      return (a.getName() || "") > (b.getName() || "") ? 1 : -1;
    });
};

const getListOfUniqueRecordNames = (
  filterTypes: RecordStatus[],
  records: ValidationStatusRecord[]
): string[] => {
  return getUniqueValues(
    getListOfFilteredRecords(filterTypes, records).map(
      (record) => record.getName() || ""
    )
  );
};
