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
import * as React from "react";
import { Layout, Menu, Typography } from "antd";
import { Link, Redirect, Route, Switch, useLocation } from "react-router-dom";

import CloudSQLImportView from "./CloudSQLImportView";
import ColumnView from "./ColumnView";
import DatasetView from "./DatasetView";
import DataFreshnessView from "./DataFreshnessView";
import TableView from "./TableView";

import * as CaseTriage from "../navigation/CaseTriage";
import * as IngestMetadata from "../navigation/IngestMetadata";

import "../style/App.css";

const App = (): JSX.Element => {
  const location = useLocation();
  const title = window.RUNTIME_GAE_ENVIRONMENT || "unknown env";

  return (
    <Layout style={{ height: "100%" }}>
      <Layout.Sider>
        <Typography.Title level={4} style={{ margin: 23 }}>
          {title.toUpperCase()}
        </Typography.Title>
        <Menu mode="inline" selectedKeys={selectedMenuKeys(location.pathname)}>
          <Menu.ItemGroup title="Ingest Metadata">
            <Menu.Item key={IngestMetadata.STATE_METADATA_ROUTE}>
              <Link to={IngestMetadata.STATE_METADATA_ROUTE}>
                State Dataset
              </Link>
            </Menu.Item>
            <Menu.Item key={IngestMetadata.DATA_FRESHNESS_ROUTE}>
              <Link to={IngestMetadata.DATA_FRESHNESS_ROUTE}>
                Data Freshness
              </Link>
            </Menu.Item>
          </Menu.ItemGroup>
          <Menu.ItemGroup title="Case Triage">
            <Menu.Item key={CaseTriage.GCS_CSV_TO_CLOUD_SQL_ROUTE}>
              <Link to={CaseTriage.GCS_CSV_TO_CLOUD_SQL_ROUTE}>
                GCS &rarr; Cloud SQL
              </Link>
            </Menu.Item>
          </Menu.ItemGroup>
        </Menu>
      </Layout.Sider>
      <div className="main-content">
        <Switch>
          <Route
            path={IngestMetadata.COLUMN_ROUTE_TEMPLATE}
            component={ColumnView}
          />
          <Route
            path={IngestMetadata.DB_ROUTE_TEMPLATE}
            component={TableView}
          />
          <Route
            exact
            path={IngestMetadata.STATE_METADATA_ROUTE}
            component={DatasetView}
          />
          <Route
            path={IngestMetadata.DATA_FRESHNESS_ROUTE}
            component={DataFreshnessView}
          />
          <Route
            path={CaseTriage.GCS_CSV_TO_CLOUD_SQL_ROUTE}
            component={CloudSQLImportView}
          />
          <Redirect from="/" to={IngestMetadata.STATE_METADATA_ROUTE} />
        </Switch>
      </div>
    </Layout>
  );
};

function selectedMenuKeys(pathname: string): string[] {
  if (pathname.startsWith(IngestMetadata.STATE_METADATA_ROUTE)) {
    return [IngestMetadata.STATE_METADATA_ROUTE];
  }
  if (pathname.startsWith(IngestMetadata.DATA_FRESHNESS_ROUTE)) {
    return [IngestMetadata.DATA_FRESHNESS_ROUTE];
  }
  if (pathname.startsWith(CaseTriage.GCS_CSV_TO_CLOUD_SQL_ROUTE)) {
    return [CaseTriage.GCS_CSV_TO_CLOUD_SQL_ROUTE];
  }
  return [];
}

export default App;
