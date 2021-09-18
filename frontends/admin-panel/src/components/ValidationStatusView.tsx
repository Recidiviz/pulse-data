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
import {
  ValidationFailureMetadataRecord,
  ValidationResultStatus,
} from "../models/ValidationModels";
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

  const failureLabelColumns: ColumnsType<ValidationFailureMetadataRecord> = [
    {
      title: "Category",
      key: "category",
      fixed: "left",
      render: (_: string, record: ValidationFailureMetadataRecord) => (
        <div>{record.category}</div>
      ),
    },
    {
      title: "Validation Name",
      key: "validation",
      fixed: "left",
      width: "35%",
      render: (_: string, record: ValidationFailureMetadataRecord) => (
        <div>{record.name}</div>
      ),
    },
    {
      title: "State",
      key: "state",
      fixed: "left",
      render: (_: string, record: ValidationFailureMetadataRecord) => (
        <div>{record.stateCode}</div>
      ),
    },
    {
      title: "Status",
      key: "status",
      fixed: "left",
      render: (_: string, record: ValidationFailureMetadataRecord) => {
        const status = record.result;
        return renderRecordStatus(status);
      },
    },
    {
      title: "Soft Threshold",
      key: "soft-failure-thresholds",
      fixed: "left",
      render: (_: string, record: ValidationFailureMetadataRecord) => (
        <div>
          {formatStatusAmount(
            record.result.softFailureAmount,
            record.result.isPercentage
          )}
        </div>
      ),
    },
    {
      title: "Hard Threshold",
      key: "hard-failure-thresholds",
      fixed: "left",
      render: (_: string, record: ValidationFailureMetadataRecord) => (
        <div>
          {formatStatusAmount(
            record.result.hardFailureAmount,
            record.result.isPercentage
          )}
        </div>
      ),
    },
  ];

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
      <Title level={3}>Table of Contents</Title>
      <Anchor affix={false}>
        <Link href="#failures" title="Failure Summary">
          <Link href="#hard-failures" title="Hard Failures" />
          <Link href="#soft-failures" title="Soft Failures" />
        </Link>
        <Link href="#full-results" title="Full Results">
          {categories.map((category) => {
            return (
              <Link
                href={`#${chooseIDNameForCategory(category)}`}
                title={readableNameForCategory(category)}
              />
            );
          })}
        </Link>
      </Anchor>
      <Title id="failures" level={1}>
        Failure Summary
      </Title>
      <Title id="hard-failures" level={2}>
        Hard Failures
      </Title>
      <Table
        className="metadata-table"
        columns={failureLabelColumns}
        dataSource={getListOfFailureRecords(
          ValidationResultStatus.FAIL_HARD,
          allStates,
          validationNames,
          data.results
        )}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          pageSize: 50,
          size: "small",
        }}
        rowClassName="metadata-table-row"
        rowKey="validation"
      />
      <Title id="soft-failures" level={2}>
        Soft Failures
      </Title>
      <Table
        className="metadata-table"
        columns={failureLabelColumns}
        dataSource={getListOfFailureRecords(
          ValidationResultStatus.FAIL_SOFT,
          allStates,
          validationNames,
          data.results
        )}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          pageSize: 50,
          size: "small",
        }}
        rowClassName="metadata-table-row"
        rowKey="validation"
      />
      <Title id="full-results" level={1}>
        Full Results
      </Title>
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

function formatStatusAmount(
  amount: number | null | undefined,
  isPercent: boolean
) {
  if (amount === null || amount === undefined) {
    return "";
  }
  if (isPercent) {
    return `${(amount * 100).toFixed(2)}%`;
  }
  return amount.toString();
}

const columnTypeForState = (
  state: string
): ColumnType<MetadataRecord<ValidationStatus>> => {
  return {
    title: state,
    key: state,
    render: (_: string, record: MetadataRecord<ValidationStatus>) => {
      const status = record.resultsByState[state];
      return renderRecordStatus(status);
    },
  };
};

const renderRecordStatus = (status: ValidationStatus) => {
  if (status === undefined) {
    return <div>No Result</div>;
  }
  if (status.didRun === false) {
    return <div className="broken">Broken</div>;
  }
  if (status.hasData === false) {
    return <div>Need Data</div>;
  }
  const { errorAmount, isPercentage } = status;

  switch (status.validationResultStatus) {
    case ValidationResultStatus.FAIL_HARD:
      return (
        <div className="failed-hard">
          Hard Fail ({formatStatusAmount(errorAmount, isPercentage)})
        </div>
      );
    case ValidationResultStatus.FAIL_SOFT:
      return (
        <div className="failed-soft">
          Soft Fail ({formatStatusAmount(errorAmount, isPercentage)})
        </div>
      );
    case ValidationResultStatus.SUCCESS:
      return (
        <div className="success">
          Passed ({formatStatusAmount(errorAmount, isPercentage)})
        </div>
      );
    default:
      return (
        <div className="broken">
          Unknown Status Result &quot;{status.validationResultStatus}&quot; (
          {formatStatusAmount(errorAmount, isPercentage)})
        </div>
      );
  }
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

const getListOfFailureRecords = (
  failureType: ValidationResultStatus,
  allStates: string[],
  validationNames: string[],
  results: { [validationName: string]: ValidationStatusResult }
): ValidationFailureMetadataRecord[] => {
  return validationNames.reduce((acc, name) => {
    const result = results[name];
    allStates.forEach((state) => {
      const resultForState = result.resultsByState[state];
      if (
        resultForState &&
        resultForState.validationResultStatus === failureType
      ) {
        acc.push({
          name,
          category: result.validationCategory,
          stateCode: state,
          result: resultForState,
        });
      }
    });

    return acc;
  }, [] as ValidationFailureMetadataRecord[]);
};
