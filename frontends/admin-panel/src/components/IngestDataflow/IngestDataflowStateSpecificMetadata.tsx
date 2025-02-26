// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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
import { Layout, Menu, MenuProps } from "antd";
import Sider from "antd/lib/layout/Sider";
import { Content } from "antd/lib/layout/layout";
import * as React from "react";
import {
  Redirect,
  Route,
  Switch,
  useHistory,
  useLocation,
  useParams,
  useRouteMatch,
} from "react-router-dom";
import {
  INGEST_DATAFLOW_INSTANCE_ROUTE,
  INGEST_DATAFLOW_PRIMARY_ROUTE,
  INGEST_DATAFLOW_SECONDARY_ROUTE,
  INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE,
} from "../../navigation/IngestOperations";
import { StateCodeInfo } from "../IngestOperationsView/constants";
import StateSelectorPageHeader from "../general/StateSelectorPageHeader";
import IngestDataflowStateSpecificInstanceMetadata from "./IngestDataflowStateSpecificInstanceMetadata";
import { ANCHOR_DATAFLOW_LATEST_JOB } from "./constants";

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

const items: MenuProps["items"] = [
  getItem("Primary Instance", INGEST_DATAFLOW_PRIMARY_ROUTE, null, [
    getItem(
      "Latest Job",
      `${INGEST_DATAFLOW_PRIMARY_ROUTE}#${ANCHOR_DATAFLOW_LATEST_JOB}`
    ),
  ]),
  getItem("Secondary Instance", INGEST_DATAFLOW_SECONDARY_ROUTE, null, [
    getItem(
      "Latest Job",
      `${INGEST_DATAFLOW_SECONDARY_ROUTE}#${ANCHOR_DATAFLOW_LATEST_JOB}`
    ),
  ]),
];

const IngestDataflowStateSpecificMetadata = (): JSX.Element => {
  const history = useHistory();
  const match = useRouteMatch();

  const location = useLocation();

  const { stateCode } = useParams<{ stateCode: string }>();

  const stateCodeChange = (value: StateCodeInfo) => {
    history.push(location.pathname.replace(stateCode, value.code));
  };

  const onClick: MenuProps["onClick"] = (e) => {
    history.push(e.key.replace(":stateCode", stateCode));
  };

  return (
    <>
      <StateSelectorPageHeader
        title="Ingest Dataflow Pipeline Status"
        onChange={stateCodeChange}
        stateCode={stateCode}
      />
      <Layout style={{ flexDirection: "row" }}>
        <Sider width={200}>
          <Menu
            style={{ marginTop: "5px" }}
            onClick={onClick}
            mode="inline"
            items={items}
          />
        </Sider>
        <Layout className="main-content">
          <Content>
            <Switch>
              <Route
                path={INGEST_DATAFLOW_INSTANCE_ROUTE}
                component={IngestDataflowStateSpecificInstanceMetadata}
              />
              <Redirect
                exact
                path={INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE}
                to={`${match.path}/instance/PRIMARY`}
              />
            </Switch>
          </Content>
        </Layout>
      </Layout>
    </>
  );
};
export default IngestDataflowStateSpecificMetadata;
