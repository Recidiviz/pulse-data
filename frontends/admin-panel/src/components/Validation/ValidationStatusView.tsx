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

import { Alert, Layout, List, Menu, MenuProps, Typography } from "antd";
import { Content } from "antd/lib/layout/layout";
import Sider from "antd/lib/layout/Sider";
import { useEffect } from "react";
import { useHistory, useLocation } from "react-router-dom";
import { fetchValidationStatus } from "../../AdminPanelAPI";
import { useFetchedDataProtobuf } from "../../hooks";
import {
  ValidationStatusRecord,
  ValidationStatusRecords,
} from "../../recidiviz/admin_panel/models/validation_pb";
import StateSelectorPageHeader from "../general/StateSelectorPageHeader";
import { StateCodeInfo } from "../IngestOperationsView/constants";
import { scrollToAnchor } from "../Utilities/GeneralUtilities";
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
  readableNameForCategoryId,
} from "./utils";
import ValidationCategoryFullResultsTable from "./ValidationCategoryFullResultsTable";
import ValidationFailureTable from "./ValidationFailureTable";

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

const getMenuItems: (
  categoryIds: string[],
  stateCode: string | null
) => MenuProps["items"] = (categoryIds: string[], stateCode: string | null) => {
  return [
    getItem(
      "Summary Failures",
      `#${ANCHOR_VALIDATION_SUMMARY_FAILURES}`,
      null,
      [
        getItem("Hard Failures", `#${ANCHOR_VALIDATION_HARD_FAILURES}`),
        getItem("Soft Failures", `#${ANCHOR_VALIDATION_SOFT_FAILURES}`),
      ]
    ),
    getItem(
      "Full Results",
      `#${ANCHOR_VALIDATION_FULL_RESULTS}`,
      null,
      stateCode
        ? categoryIds.map((categoryId) => {
            return getItem(
              readableNameForCategoryId(categoryId),
              `#${categoryId}`
            );
          })
        : undefined
    ),
  ];
};

const ValidationStatusView = (): JSX.Element => {
  const history = useHistory();
  const { pathname, hash, search } = useLocation();
  const queryParams = new URLSearchParams(search);
  const stateCode = queryParams.get("stateCode");

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
    history.push({
      pathname,
      hash: e.key,
      search: queryParams.toString(),
    });
  };

  const stateCodeChange = (value: StateCodeInfo) => {
    queryParams.set("stateCode", value.code);
    history.push({ search: queryParams.toString() });
  };

  return (
    <Layout style={{ height: "100%", width: "100%" }}>
      <StateSelectorPageHeader
        title="Validation Status"
        subTitle="Shows the current status of each validation for each state."
        stateCode={stateCode}
        onChange={stateCodeChange}
      />
      <Layout style={{ flexDirection: "row" }}>
        <Sider width={200}>
          <Menu
            style={{ marginTop: "5px" }}
            onClick={onClick}
            mode="inline"
            selectedKeys={[pathname + hash]}
            items={getMenuItems(categoryIds, stateCode)}
            defaultOpenKeys={getMenuItems([], stateCode)?.map(
              (x) => x?.key?.toString() || ""
            )}
          />
        </Sider>
        <Layout className="main-content content-side-padding">
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
            <ValidationFailureTable
              statuses={[RecordStatus.FAIL_HARD, RecordStatus.BROKEN]}
              records={records}
              allStates={allStates}
              selectedState={stateCode}
              categoryIds={categoryIds}
              loading={loading}
            />
            <Title id={ANCHOR_VALIDATION_SOFT_FAILURES} level={2}>
              Soft Failures
            </Title>
            <ValidationFailureTable
              statuses={[RecordStatus.FAIL_SOFT]}
              records={records}
              allStates={allStates}
              selectedState={stateCode}
              categoryIds={categoryIds}
              loading={loading}
            />
            <Title id={ANCHOR_VALIDATION_FULL_RESULTS} level={1}>
              Full Results
            </Title>
            {stateCode ? (
              categoryIds.sort().map((categoryId) => {
                return (
                  <>
                    <Title id={categoryId} level={2}>
                      {readableNameForCategoryId(categoryId)}
                    </Title>
                    <ValidationCategoryFullResultsTable
                      selectedStates={stateCode ? [stateCode] : allStates}
                      categoryId={categoryId}
                      dictOfCategoryIdsToRecords={dictOfCategoryIdsToRecords}
                      loading={loading}
                    />
                  </>
                );
              })
            ) : (
              <Alert
                message="Select a region to view region-specific validation category details"
                type="warning"
                showIcon
              />
            )}
          </Content>
        </Layout>
      </Layout>
    </Layout>
  );
};

export default ValidationStatusView;
