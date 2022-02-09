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

import { InfoCircleOutlined } from "@ant-design/icons";
import {
  Anchor,
  Breadcrumb,
  List,
  PageHeader,
  Spin,
  Table,
  Tooltip,
  Typography,
} from "antd";
import { ColumnsType, ColumnType } from "antd/es/table";
import classNames from "classnames";
import { History } from "history";
import * as React from "react";
import { useHistory } from "react-router-dom";
import { MouseEventHandler } from "react-router/node_modules/@types/react";
import { fetchValidationStatus } from "../../AdminPanelAPI";
import { useFetchedDataProtobuf } from "../../hooks";
import { routeForValidationDetail } from "../../navigation/DatasetMetadata";
import {
  ValidationStatusRecord,
  ValidationStatusRecords,
} from "../../recidiviz/admin_panel/models/validation_pb";
import uniqueStates from "../Utilities/UniqueStates";
import { RecordStatus } from "./constants";
import {
  chooseIdNameForCategory,
  formatDatetime,
  formatStatusAmount,
  getClassNameForRecordStatus,
  getDaysActive,
  getRecordStatus,
  getTextForRecordStatus,
  readableNameForCategoryId,
  replaceInfinity,
} from "./utils";

const { Title } = Typography;

interface MetadataItem {
  key: string;
  value?: string;
}

const ValidationStatusView = (): JSX.Element => {
  const history = useHistory();

  const { loading, data } = useFetchedDataProtobuf<ValidationStatusRecords>(
    fetchValidationStatus,
    ValidationStatusRecords.deserializeBinary
  );

  const records = data?.getRecordsList() || [];

  const recordsByName = records.reduce((acc, record) => {
    const name = record.getName() || "";
    const metadataRecord = acc[name] || { name, resultsByState: {} };
    metadataRecord.resultsByState[record.getStateCode() || ""] = record;
    acc[name] = metadataRecord;
    return acc;
  }, {} as { [name: string]: MetadataRecord<ValidationStatusRecord> });

  const validationNames = Object.keys(recordsByName).sort();
  const allStates = uniqueStates(Object.values(recordsByName));

  const dictOfCategoryIdsToRecords = validationNames.reduce((acc, name) => {
    const result = recordsByName[name];
    const metadataRecord: MetadataRecord<ValidationStatusRecord> = {
      name,
      resultsByState: result.resultsByState,
    };
    const category = chooseIdNameForCategory(
      Object.values(result.resultsByState)[0].getCategory()
    );
    const recordsForCategory = acc[category] || [];
    acc[category] = [...recordsForCategory, metadataRecord];
    return acc;
  }, {} as { [category: string]: MetadataRecord<ValidationStatusRecord>[] });

  const categoryIds = Object.keys(dictOfCategoryIdsToRecords).sort();

  const failureLabelColumns: ColumnsType<ValidationStatusRecord> = [
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
    },
    {
      title: "State",
      key: "state",
      fixed: "left",
      render: (_: string, record: ValidationStatusRecord) => (
        <div>{record.getStateCode()}</div>
      ),
      filters: allStates.map((state: string) => ({
        text: state,
        value: state,
      })),
      onFilter: (value, record: ValidationStatusRecord) =>
        record.getStateCode() === value,
    },
    {
      title: "Status",
      key: "status",
      fixed: "left",
      render: (_: string, record: ValidationStatusRecord) => {
        return renderRecordStatus(record);
      },
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

  const labelColumns: ColumnsType<MetadataRecord<ValidationStatusRecord>> = [
    {
      title: "Validation Name",
      key: "validation",
      fixed: "left",
      width: "55%",
      onCell: (record) => {
        return {
          onClick: handleClickToDetails(history, record.name),
        };
      },
      render: (_: string, record: MetadataRecord<ValidationStatusRecord>) => (
        <div>{record.name}</div>
      ),
    },
  ];

  const columns = labelColumns.concat(
    allStates.map((s) => columnTypeForState(s, history))
  );

  const initialRecord = records.length > 0 ? records[0] : undefined;
  const metadata: MetadataItem[] = [
    { key: "Run Id", value: initialRecord?.getRunId() },
    {
      key: "Run Datetime",
      value: formatDatetime(initialRecord?.getRunDatetime()?.toDate()),
    },
    { key: "System Version", value: initialRecord?.getSystemVersion() },
  ];

  return (
    <>
      <Breadcrumb>
        <Breadcrumb.Item>Validation Status</Breadcrumb.Item>
      </Breadcrumb>
      <PageHeader
        title="Validation Status"
        subTitle="Shows the current status of each validation for each state."
      />
      <List
        size="small"
        dataSource={metadata}
        loading={loading}
        renderItem={(item: MetadataItem) => (
          <List.Item>
            {item.key}: {item.value}
          </List.Item>
        )}
      />
      <Title level={3}>Table of Contents</Title>
      <Anchor affix={false}>
        <div>
          <Anchor.Link
            className="validation-anchor-link"
            href="#summary-failures"
            title="Failure Summary"
          />
        </div>
        <div className="validation-anchor-link-child validation-anchor-link">
          <Anchor.Link href="#hard-failures" title="Hard Failures" />
        </div>
        <div className="validation-anchor-link-child validation-anchor-link">
          <Anchor.Link href="#soft-failures" title="Soft Failures" />
        </div>

        <div className="validation-anchor-link">
          <Anchor.Link href="#full-results" title="Full Results" />
        </div>
        {loading ? (
          <Spin />
        ) : (
          categoryIds.map((categoryId) => {
            return (
              <div className="validation-anchor-link-child validation-anchor-link">
                <Anchor.Link
                  href={`#${categoryId}`}
                  title={readableNameForCategoryId(categoryId)}
                />
              </div>
            );
          })
        )}
      </Anchor>

      <Title id="summary-failures" level={1}>
        Failure Summary
      </Title>
      <Title id="hard-failures" level={2}>
        Hard Failures
      </Title>
      <Table
        className="validation-table"
        columns={failureLabelColumns}
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
        dataSource={getListOfFailureRecords(
          ValidationStatusRecord.ValidationResultStatus.FAIL_HARD,
          records
        )}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          pageSize: 50,
          size: "small",
        }}
        rowClassName="validation-table-row"
        rowKey="validation"
      />
      <Title id="soft-failures" level={2}>
        Soft Failures
      </Title>
      <Table
        className="validation-table"
        columns={failureLabelColumns}
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
        dataSource={getListOfFailureRecords(
          ValidationStatusRecord.ValidationResultStatus.FAIL_SOFT,
          records
        )}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          pageSize: 50,
          size: "small",
        }}
        rowClassName="validation-table-row"
        rowKey="validation"
      />
      <Title id="full-results" level={1}>
        Full Results
      </Title>
      {categoryIds.sort().map((categoryId) => {
        return (
          <>
            <Title id={categoryId} level={2}>
              {readableNameForCategoryId(categoryId)}
            </Title>
            <Table
              className="validation-table"
              columns={columns}
              loading={loading}
              dataSource={dictOfCategoryIdsToRecords[categoryId]}
              pagination={{
                hideOnSinglePage: true,
                showSizeChanger: true,
                pageSize: 50,
                size: "small",
              }}
              rowClassName="validation-table-row"
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
  state: string,
  history: History
): ColumnType<MetadataRecord<ValidationStatusRecord>> => {
  return {
    title: state,
    key: state,
    onCell: (record) => {
      return {
        onClick: handleClickToDetails(history, record.name, state),
      };
    },
    render: (_: string, record: MetadataRecord<ValidationStatusRecord>) => {
      return renderRecordStatus(record.resultsByState[state]);
    },
  };
};

const renderRecordStatus = (record: ValidationStatusRecord | undefined) => {
  const status = getRecordStatus(record);
  const statusClassName = getClassNameForRecordStatus(status);
  const text = getTextForRecordStatus(status);
  const body =
    text +
    (status > RecordStatus.NEED_DATA
      ? ` (${formatStatusAmount(
          record?.getErrorAmount(),
          record?.getIsPercentage()
        )})`
      : "");
  return (
    <div
      className={classNames(statusClassName, {
        "dev-mode": !record?.getDevMode(),
      })}
    >
      {body}
      {record?.getDevMode() && (
        <div>
          <em>Dev Mode</em>
        </div>
      )}
    </div>
  );
};

const handleClickToDetails = (
  history: History,
  validationName?: string,
  stateCode?: string
): MouseEventHandler => {
  return (_event) =>
    history.push({
      pathname: routeForValidationDetail(validationName),
      search: stateCode && `?stateCode=${stateCode}`,
    });
};

const getListOfFailureRecords = (
  failureType: ValidationStatusRecord.ValidationResultStatusMap[keyof ValidationStatusRecord.ValidationResultStatusMap],
  records: ValidationStatusRecord[]
): ValidationStatusRecord[] => {
  return records
    .filter(
      (record: ValidationStatusRecord) =>
        record?.getDevMode() && record.getResultStatus() === failureType
    )
    .sort((a, b) => {
      if (a.getName() === b.getName()) {
        return (a.getStateCode() || "") > (b.getStateCode() || "") ? 1 : -1;
      }
      return (a.getName() || "") > (b.getName() || "") ? 1 : -1;
    });
};
