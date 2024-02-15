// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2020 Recidiviz, Inc.
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

import { Checkbox, Empty, Table } from "antd";
import { CheckboxChangeEvent } from "antd/es/checkbox";
import { ColumnsType } from "antd/es/table";
import { useState } from "react";
import { Link } from "react-router-dom";

import { addStateCodeQueryToLink } from "../navigation/DatasetMetadata";
import { MetadataAPIResult, MetadataCount, MetadataRecord } from "../types";

const emptyCell = <div className="center">N/A</div>;

const getCountForCountedRecord = (
  countedRecord: MetadataCount | undefined,
  nonplaceholdersOnly: boolean
): number => {
  if (!countedRecord) {
    return 0;
  }

  return nonplaceholdersOnly && countedRecord.placeholderCount
    ? countedRecord.totalCount - countedRecord.placeholderCount
    : countedRecord.totalCount;
};

interface MetadataTableProps {
  initialColumnTitle: string;
  stateCode?: string | null;
  initialColumnLink?: (name: string) => string;
  data: MetadataAPIResult | undefined;
}

const MetadataTable = (props: MetadataTableProps): JSX.Element => {
  const { data, stateCode, initialColumnLink, initialColumnTitle } = props;
  const [nonplaceholdersOnly, setNonplaceholdersOnly] = useState<boolean>(true);

  if (data === undefined || !stateCode) {
    return <Empty className="buffer" />;
  }
  const metadataRecords = Object.keys(data)
    .sort()
    .map((name: string): MetadataRecord<MetadataCount> => {
      return {
        name,
        resultsByState: data[name],
      };
    });
  if (metadataRecords.length === 0) {
    return <Empty className="buffer" />;
  }

  const columns: ColumnsType<MetadataRecord<MetadataCount>> = [
    {
      title: initialColumnTitle,
      key: "name",
      fixed: "left",
      render: (_: string, record: MetadataRecord<MetadataCount>) => {
        if (initialColumnLink === undefined) {
          return record.name;
        }
        return (
          <div>
            <Link
              to={addStateCodeQueryToLink(
                initialColumnLink(record.name),
                stateCode
              )}
            >
              {record.name}
            </Link>
          </div>
        );
      },
      sorter: (a, b) => a.name.localeCompare(b.name),
      defaultSortOrder: "ascend",
      filters: metadataRecords.map(({ name }) => ({
        text: name,
        value: name,
      })),
      onFilter: (value, content) => content.name === value,
      filterSearch: true,
    },
    {
      title: "Count",
      key: "count",
      render: (_: string, record: MetadataRecord<MetadataCount>) => {
        const countedRecord = record.resultsByState[stateCode];
        if (countedRecord === undefined) {
          return emptyCell;
        }
        const count = getCountForCountedRecord(
          countedRecord,
          nonplaceholdersOnly
        );
        if (count === 0) {
          return emptyCell;
        }
        return <div className="success">{count.toLocaleString()}</div>;
      },
      sorter: (a, b) =>
        getCountForCountedRecord(
          a.resultsByState[stateCode],
          nonplaceholdersOnly
        ) -
        getCountForCountedRecord(
          b.resultsByState[stateCode],
          nonplaceholdersOnly
        ),
    },
  ];

  const onChange = (e: CheckboxChangeEvent) => {
    setNonplaceholdersOnly(e.target.checked);
  };

  const hasPlaceholders = Object.keys(data).reduce(
    (nameAccumulator, name) =>
      nameAccumulator ||
      Object.keys(data[name]).reduce(
        (stateAccumulator, state) =>
          stateAccumulator || data[name][state].placeholderCount !== undefined,
        false
      ),
    false
  );
  return (
    <>
      {hasPlaceholders && (
        <Checkbox
          className="buffer"
          onChange={onChange}
          checked={nonplaceholdersOnly}
        >
          Non-placeholders only
        </Checkbox>
      )}
      <div>
        <Table
          className="metadata-table"
          dataSource={metadataRecords}
          columns={columns}
          pagination={{
            hideOnSinglePage: true,
            showSizeChanger: true,
            pageSize: 50,
            size: "small",
          }}
          rowClassName="metadata-table-row"
          rowKey="name"
        />
      </div>
    </>
  );
};

export default MetadataTable;
