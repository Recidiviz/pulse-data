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

import { Layout, List, Menu, MenuProps } from "antd";
import Sider from "antd/lib/layout/Sider";
import { Content } from "antd/lib/layout/layout";
import { FC, useEffect } from "react";
import {
  Redirect,
  Route,
  Switch,
  useHistory,
  useLocation,
} from "react-router-dom";
import { fetchValidationStatus } from "../../../AdminPanelAPI";
import { useFetchedDataProtobuf } from "../../../hooks";
import {
  VALIDATION_STATUS_FAILURE_SUMMARY_ROUTE,
  VALIDATION_STATUS_FULL_RESULTS_ROUTE,
} from "../../../navigation/DatasetMetadata";
import {
  ValidationStatusRecord,
  ValidationStatusRecords,
} from "../../../recidiviz/admin_panel/models/validation_pb";
import { MetadataRecord } from "../../../types";
import {
  formatDatetime,
  scrollToAnchor,
} from "../../Utilities/GeneralUtilities";
import uniqueStates from "../../Utilities/UniqueStates";
import StateSelectorPageHeader from "../../general/StateSelectorPageHeader";
import { StateCodeInfo } from "../../general/constants";
import {
  ANCHOR_VALIDATION_FAILURE_SUMMARY,
  ANCHOR_VALIDATION_FULL_RESULTS,
  ANCHOR_VALIDATION_HARD_FAILURES,
  ANCHOR_VALIDATION_SOFT_FAILURES,
} from "../constants";
import { chooseIdNameForCategory, readableNameForCategoryId } from "../utils";
import ValidationFailureSummary from "./ValidationFailureSummary";
import ValidationFullResults from "./ValidationFullResults";

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
    getItem("Failure Summary", ANCHOR_VALIDATION_FAILURE_SUMMARY, null, [
      getItem("Hard Failures", ANCHOR_VALIDATION_HARD_FAILURES),
      getItem("Soft Failures", ANCHOR_VALIDATION_SOFT_FAILURES),
    ]),
    getItem(
      "Full Results",
      ANCHOR_VALIDATION_FULL_RESULTS,
      null,
      stateCode
        ? categoryIds.map((categoryId) => {
            return getItem(readableNameForCategoryId(categoryId), categoryId);
          })
        : undefined
    ),
  ];
};

const getIdToUrlMap: (
  categoryIds: string[],
  stateCode: string | null
) => Map<string, { pathname: string; hash?: string }> = (
  categoryIds: string[],
  stateCode: string | null
) => {
  const links: [string, { pathname: string; hash?: string }][] = [
    [
      ANCHOR_VALIDATION_FAILURE_SUMMARY,
      {
        pathname: VALIDATION_STATUS_FAILURE_SUMMARY_ROUTE,
      },
    ],
    [
      ANCHOR_VALIDATION_HARD_FAILURES,
      {
        pathname: VALIDATION_STATUS_FAILURE_SUMMARY_ROUTE,
        hash: `#${ANCHOR_VALIDATION_HARD_FAILURES}`,
      },
    ],
    [
      ANCHOR_VALIDATION_SOFT_FAILURES,
      {
        pathname: VALIDATION_STATUS_FAILURE_SUMMARY_ROUTE,
        hash: `#${ANCHOR_VALIDATION_SOFT_FAILURES}`,
      },
    ],
    [
      ANCHOR_VALIDATION_FULL_RESULTS,
      {
        pathname: VALIDATION_STATUS_FULL_RESULTS_ROUTE,
      },
    ],
  ];
  const fullResultsLinks: [string, { pathname: string; hash?: string }][] =
    stateCode
      ? categoryIds.map((categoryId) => [
          categoryId,
          {
            pathname: VALIDATION_STATUS_FULL_RESULTS_ROUTE,
            hash: `#${categoryId}`,
          },
        ])
      : [];

  return new Map<string, { pathname: string; hash?: string }>(
    links.concat(fullResultsLinks)
  );
};

interface ValdiationStatusViewProps {
  stateCode: string | null;
  stateCodeChange: (value: StateCodeInfo) => void;
}

const ValidationStatusView: FC<ValdiationStatusViewProps> = ({
  stateCode,
  stateCodeChange,
}) => {
  const history = useHistory();
  const { pathname, hash, search } = useLocation();

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
    const map = getIdToUrlMap(categoryIds, stateCode);
    const link = map.get(e.key);
    if (!link) {
      throw new Error(`Could not find link map for: ${e.key}`);
    }
    history.push({
      pathname: link.pathname,
      hash: link.hash,
      search,
    });
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
            selectedKeys={[getIdForUrl(pathname, categoryIds, stateCode, hash)]}
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
            <Switch>
              <Route path={VALIDATION_STATUS_FAILURE_SUMMARY_ROUTE}>
                <ValidationFailureSummary
                  stateCode={stateCode}
                  categoryIds={categoryIds}
                  allStates={allStates}
                  records={records}
                  loading={loading}
                />
              </Route>
              <Route path={VALIDATION_STATUS_FULL_RESULTS_ROUTE}>
                <ValidationFullResults
                  stateCode={stateCode}
                  categoryIds={categoryIds}
                  allStates={allStates}
                  dictOfCategoryIdsToRecords={dictOfCategoryIdsToRecords}
                  loading={loading}
                />
              </Route>
              <Redirect to={VALIDATION_STATUS_FAILURE_SUMMARY_ROUTE} />
            </Switch>
          </Content>
        </Layout>
      </Layout>
    </Layout>
  );
};

export default ValidationStatusView;

function getIdForUrl(
  pathname: string,
  categoryIds: string[],
  stateCode: string | null,
  hash?: string
): string {
  const map = getIdToUrlMap(categoryIds, stateCode);
  const entries = Array.from(map.entries());
  const link = entries.find(
    ([_, mapLink]) =>
      mapLink.pathname === pathname && (mapLink.hash === hash || hash === "")
  );
  return link ? link[0] : "";
}
