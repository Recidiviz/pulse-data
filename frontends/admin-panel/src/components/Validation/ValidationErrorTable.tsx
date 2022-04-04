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

import { Alert, Table } from "antd";
import { ColumnsType, ColumnType } from "antd/lib/table";
import * as React from "react";
import { Link } from "react-router-dom";
import {
  LOOKER_PERSON_DETAILS_PROD,
  LOOKER_PERSON_DETAILS_STAGING,
} from "../../navigation/looker";
import {
  ValidationErrorTableProps,
  ValidationErrorTableRows,
} from "./constants";

const getErrorTableColumns = (
  tableRows: ValidationErrorTableRows[]
): ColumnsType<ValidationErrorTableRows> => {
  return tableRows.length > 0
    ? Object.keys(tableRows[0]).map(mapColumnNameToColumn)
    : [];
};

const mapColumnNameToColumn = (
  columnName: string
): ColumnType<ValidationErrorTableRows> => {
  return {
    title: columnName,
    key: columnName,
    render: (_: string, record: ValidationErrorTableRows) =>
      renderCell(record, columnName),
  };
};

const renderCell = (record: ValidationErrorTableRows, columnName: string) => {
  if (columnName.match(/.*person_id.*/) && record[columnName]) {
    return (
      <div>
        <Link
          to={createPersonIdLinkToLookerPersonDetailsDashbaord(
            record[columnName].toString()
          )}
          target="_blank"
        >
          {record[columnName]}
        </Link>
      </div>
    );
  }
  if (columnName.match(/.*person_external_id.*/) && record[columnName]) {
    return (
      <div>
        <Link
          to={createExternalPersonIdLinkToLookerPersonDetailsDashbaord(
            record[columnName].toString(),
            record.region_code.toString()
          )}
          target="_blank"
        >
          {record[columnName]}
        </Link>
      </div>
    );
  }
  return (
    <div
      style={{
        width: "max-content",
      }}
    >
      {record[columnName]}
    </div>
  );
};

const createPersonIdLinkToLookerPersonDetailsDashbaord = (personId: string) => {
  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  return {
    pathname: isProduction
      ? LOOKER_PERSON_DETAILS_PROD
      : LOOKER_PERSON_DETAILS_STAGING,
    search: `?Person+ID=${personId}`,
  };
};

const createExternalPersonIdLinkToLookerPersonDetailsDashbaord = (
  externalPersonId: string,
  stateCode: string
) => {
  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  return {
    pathname: isProduction
      ? LOOKER_PERSON_DETAILS_PROD
      : LOOKER_PERSON_DETAILS_STAGING,
    search: `?External+ID=${externalPersonId}&State+Code=${stateCode}`,
  };
};

const ValidationErrorTable: React.FC<ValidationErrorTableProps> = ({
  tableData,
}) => {
  return (
    <>
      {tableData.metadata.limitedRowsShown ? (
        <Alert
          message={`Only showing first ${tableData.rows.length} of
            ${tableData.metadata.totalRows} rows. Use this query to explore further:`}
          description={<code>{tableData.metadata.query}</code>}
          type="warning"
          showIcon
        />
      ) : (
        <></>
      )}
      <Table
        className="validation-error-table"
        columns={getErrorTableColumns(tableData.rows)}
        dataSource={tableData.rows}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          pageSize: 50,
          size: "small",
        }}
        rowClassName="validation-error-table-row"
        rowKey="validation-table"
      />
    </>
  );
};

export default ValidationErrorTable;
