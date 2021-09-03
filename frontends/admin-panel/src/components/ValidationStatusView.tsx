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

import { Anchor, Empty, List, PageHeader, Spin, Table, Typography } from "antd";
import { ColumnsType, ColumnType } from "antd/es/table";
import * as React from "react";
import { fetchValidationStatus } from "../AdminPanelAPI";
import useFetchedData from "../hooks";
import { ValidationResultStatus } from "../models/ValidationModels";
import uniqueStates from "./Utilities/UniqueStates";

const { Title } = Typography;
const { Link } = Anchor;

interface MetadataItem {
  key: string;
  value: string;
}

const ValidationStatusView = (): JSX.Element => {
  const { loading, data } = useFetchedData<ValidationStatusResults>(
    fetchValidationStatus
  );

  if (data === undefined) {
    return <Empty className="buffer" />;
  }

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  const validationNames = Object.keys(data.results).sort();

  const dictOfCategoriesToRecords = validationNames.reduce((acc, name) => {
    const result = data.results[name];
    const metadataRecord: MetadataRecord<ValidationStatus> = {
      name,
      resultsByState: result.resultsByState,
    };
    const recordsForCategory = acc[result.validationCategory] || [];
    acc[result.validationCategory] = [...recordsForCategory, metadataRecord];
    return acc;
  }, {} as { [category: string]: MetadataRecord<ValidationStatus>[] });

  const labelColumns: ColumnsType<MetadataRecord<ValidationStatus>> = [
    {
      title: "Validation Name",
      key: "validation",
      fixed: "left",
      width: "55%",
      render: (_: string, record: MetadataRecord<ValidationStatus>) => (
        <div>{record.name}</div>
      ),
    },
  ];
  const allStates = uniqueStates(
    Object.values(dictOfCategoriesToRecords).flat()
  );

  const columns = labelColumns.concat(
    allStates.map((s) => columnTypeForState(s))
  );

  const metadata: MetadataItem[] = [
    { key: "Run ID", value: data.runId },
    { key: "Run Datetime", value: data.runDatetime },
    { key: "System Version", value: data.systemVersion },
  ];

  const categories = Object.keys(dictOfCategoriesToRecords).sort();

  return (
    <>
      <PageHeader
        title="Validation Status"
        subTitle="Shows the current status of each validation for each state."
      />
      <List
        size="small"
        dataSource={metadata}
        renderItem={(item: MetadataItem) => (
          <List.Item>
            {item.key}: {item.value}
          </List.Item>
        )}
      />
      <Title level={3}>Category Links</Title>
      <Anchor affix={false}>
        {categories.map((category) => {
          return (
            <Link
              href={`#${chooseIDNameForCategory(category)}`}
              title={readableNameForCategory(category)}
            />
          );
        })}
      </Anchor>
      {categories.sort().map((category) => {
        return (
          <>
            <Title id={chooseIDNameForCategory(category)} level={2}>
              {readableNameForCategory(category)}
            </Title>
            <Table
              className="metadata-table"
              columns={columns}
              dataSource={dictOfCategoriesToRecords[category]}
              pagination={{
                hideOnSinglePage: true,
                showSizeChanger: true,
                pageSize: 50,
                size: "small",
              }}
              rowClassName="metadata-table-row"
              rowKey="validation"
            />
          </>
        );
      })}
    </>
  );
};

export default ValidationStatusView;

const columnTypeForState = (
  state: string
): ColumnType<MetadataRecord<ValidationStatus>> => {
  return {
    title: state,
    key: state,
    render: (_: string, record: MetadataRecord<ValidationStatus>) => {
      const status = record.resultsByState[state];
      if (status === undefined) {
        return <div>No Result</div>;
      }
      if (status.didRun === false) {
        return <div className="broken">Broken</div>;
      }
      if (status.hasData === false) {
        return <div>Need Data</div>;
      }

      switch (status.validationResultStatus) {
        case ValidationResultStatus.FAIL_HARD:
          return (
            <div className="failed-hard">Hard Fail ({status.errorAmount})</div>
          );
        case ValidationResultStatus.FAIL_SOFT:
          return (
            <div className="failed-soft">Soft Fail ({status.errorAmount})</div>
          );
        case ValidationResultStatus.SUCCESS:
          return <div className="success">Passed ({status.errorAmount})</div>;
        default:
          return (
            <div className="broken">
              Unkown Status Result &quot;{status.validationResultStatus}&quot; (
              {status.errorAmount})
            </div>
          );
      }
    },
  };
};

const readableNameForCategory = (category: string): string => {
  switch (category) {
    case "CONSISTENCY":
      return "Consistency";
    case "EXTERNAL_AGGREGATE":
      return "External Aggregate";
    case "EXTERNAL_INDIVIDUAL":
      return "External Individual";
    case "INVARIANT":
      return "Invariant";
    case "FRESHNESS":
      return "Freshness";
    default:
      return category;
  }
};

const chooseIDNameForCategory = (category: string): string => {
  switch (category) {
    case "CONSISTENCY":
      return "CONSISTENCY";
    case "EXTERNAL_AGGREGATE":
      return "EXTERNAL_AGGREGATE";
    case "EXTERNAL_INDIVIDUAL":
      return "EXTERNAL_INDIVIDUAL";
    case "INVARIANT":
      return "INVARIANT";
    case "FRESHNESS":
      return "FRESHNESS";
    default:
      return category;
  }
};
