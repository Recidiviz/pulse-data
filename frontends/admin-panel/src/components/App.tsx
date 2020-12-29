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

import ColumnView from "./ColumnView";
import TableView from "./TableView";
import DatasetView from "./DatasetView";

import * as IngestMetadata from "../navigation/IngestMetadata";

import "../style/App.css";

const App = (): JSX.Element => {
  const location = useLocation();

  return (
    <Layout style={{ height: "100%" }}>
      <Layout.Sider>
        <Typography.Title level={4} style={{ margin: 23 }}>
          Admin Panel
        </Typography.Title>
        <Menu mode="inline" selectedKeys={selectedMenuKeys(location.pathname)}>
          <Menu.Item key={IngestMetadata.ROOT_ROUTE}>
            <Link to={IngestMetadata.ROOT_ROUTE}>Ingest Metadata</Link>
          </Menu.Item>
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
            path={IngestMetadata.ROOT_ROUTE}
            component={DatasetView}
          />
          <Redirect from="/" to={IngestMetadata.ROOT_ROUTE} />
        </Switch>
      </div>
    </Layout>
  );
};

function selectedMenuKeys(pathname: string): string[] {
  if (pathname.startsWith(IngestMetadata.ROOT_ROUTE)) {
    return [IngestMetadata.ROOT_ROUTE];
  }
  return [];
}

export default App;
