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
  Layout,
  List,
  Menu,
  MenuProps,
  PageHeader,
  Table,
  Tooltip,
  Typography,
} from "antd";
import { ColumnsType, ColumnType } from "antd/es/table";
import { Content } from "antd/lib/layout/layout";
import Sider from "antd/lib/layout/Sider";
import classNames from "classnames";
import { History } from "history";
import { useEffect } from "react";
import { useHistory, useLocation } from "react-router-dom";
import { MouseEventHandler } from "react-router/node_modules/@types/react";
import { fetchValidationStatus } from "../../AdminPanelAPI";
import { useFetchedDataProtobuf } from "../../hooks";
import {
  routeForValidationDetail,
  VALIDATION_STATUS_ROUTE,
} from "../../navigation/DatasetMetadata";
import {
  ValidationStatusRecord,
  ValidationStatusRecords,
} from "../../recidiviz/admin_panel/models/validation_pb";
import {
  getUniqueValues,
  optionalNumberSort,
  optionalStringSort,
  scrollToAnchor,
} from "../Utilities/GeneralUtilities";
import uniqueStates from "../Utilities/UniqueStates";
import {
  ANCHOR_VALIDATION_FULL_RESULTS,
  ANCHOR_VALIDATION_HARD_FAILURES,
  ANCHOR_VALIDATION_SOFT_FAILURES,
  ANCHOR_VALIDATION_SUMMARY_FAILURES,
  RecordStatus,
} from "./constants";
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

type MenuItem = Required<MenuProps>["items"][number];

function getItem(
  label: React.ReactNode,
  key: React.Key,
  icon?: React.ReactNode,
  children?: MenuItem[],
  type?: "group"
): MenuItem {
  return {
    key,
    icon,
    children,
    label,
    type,
  } as MenuItem;
}

const getMenuItems: (categoryIds: string[]) => MenuProps["items"] = (
  categoryIds: string[]
) => {
  return [
    getItem(
      "Summary Failures",
      `${VALIDATION_STATUS_ROUTE}#${ANCHOR_VALIDATION_SUMMARY_FAILURES}`,
      null,
      [
        getItem(
          "Hard Failures",
          `${VALIDATION_STATUS_ROUTE}#${ANCHOR_VALIDATION_HARD_FAILURES}`
        ),
        getItem(
          "Soft Failures",
          `${VALIDATION_STATUS_ROUTE}#${ANCHOR_VALIDATION_SOFT_FAILURES}`
        ),
      ]
    ),
    getItem(
      "Full Results",
      `${VALIDATION_STATUS_ROUTE}#${ANCHOR_VALIDATION_FULL_RESULTS}`,
      null,
      categoryIds.map((categoryId) => {
        return getItem(
          readableNameForCategoryId(categoryId),
          `${VALIDATION_STATUS_ROUTE}#${categoryId}`
        );
      })
    ),
  ];
};

const ValidationStatusView = (): JSX.Element => {
  const history = useHistory();
  const { pathname, hash } = useLocation();

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

  function getFailureTable(statuses: RecordStatus[]) {
    return (
      <Table
        className="validation-table"
        columns={getFailureLabelColumns(statuses)}
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
        rowKey="validation"
      />
    );
  }

  function getFailureLabelColumns(
    statuses: RecordStatus[]
  ): ColumnsType<ValidationStatusRecord> {
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

  function columnTypeForState(
    categoryId: string,
    state: string
  ): ColumnType<MetadataRecord<ValidationStatusRecord>> {
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

  function getLabelColumns(
    categoryId: string
  ): ColumnsType<MetadataRecord<ValidationStatusRecord>> {
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
      allStates.map((s) => columnTypeForState(categoryId, s))
    );
  }

  const initialRecord = records.length > 0 ? records[0] : undefined;
  const metadata: MetadataItem[] = [
    { key: "Run Id", value: initialRecord?.getRunId() },
    {
      key: "Run Datetime",
      value: formatDatetime(initialRecord?.getRunDatetime()?.toDate()),
    },
    { key: "System Version", value: initialRecord?.getSystemVersion() },
  ];

  useEffect(() => {
    scrollToAnchor(hash);
  }, [hash, data]);

  const onClick: MenuProps["onClick"] = (e) => {
    history.push(e.key);
  };

  return (
    <Layout style={{ height: "100%", width: "100%" }}>
      <PageHeader
        title="Validation Status"
        subTitle="Shows the current status of each validation for each state."
      />
      <Layout style={{ flexDirection: "row" }}>
        <Sider width={200}>
          <Menu
            style={{ marginTop: "5px" }}
            onClick={onClick}
            mode="inline"
            selectedKeys={[pathname + hash]}
            items={getMenuItems(categoryIds)}
            defaultOpenKeys={getMenuItems([])?.map(
              (x) => x?.key?.toString() || ""
            )}
          />
        </Sider>
        <Layout className="main-content" style={{ padding: "0 24px 24px" }}>
          <Content>
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
            <Title id={ANCHOR_VALIDATION_SUMMARY_FAILURES} level={1}>
              Failure Summary
            </Title>
            <Title id={ANCHOR_VALIDATION_HARD_FAILURES} level={2}>
              Hard Failures
            </Title>
            {getFailureTable([RecordStatus.FAIL_HARD, RecordStatus.BROKEN])}
            <Title id={ANCHOR_VALIDATION_SOFT_FAILURES} level={2}>
              Soft Failures
            </Title>
            {getFailureTable([RecordStatus.FAIL_SOFT])}
            <Title id={ANCHOR_VALIDATION_FULL_RESULTS} level={1}>
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
                    columns={getLabelColumns(categoryId)}
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
          </Content>
        </Layout>
      </Layout>
    </Layout>
  );
};

export default ValidationStatusView;

const renderRecordStatus = (record: ValidationStatusRecord | undefined) => {
  const status = getRecordStatus(record);
  const statusClassName = getClassNameForRecordStatus(status);
  const text = getTextForRecordStatus(status);
  // If we don't have data so the validation didn't run, we don't need to add the dev
  // mode treatment.
  const devMode = record?.getDevMode() && status > RecordStatus.NEED_DATA;
  const body =
    text +
    (status > RecordStatus.NEED_DATA
      ? ` (${formatStatusAmount(
          record?.getErrorAmount(),
          record?.getIsPercentage()
        )})`
      : "");
  return (
    <div className={classNames(statusClassName, { "dev-mode": devMode })}>
      {body}
      {devMode && (
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
