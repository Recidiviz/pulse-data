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
import { Alert } from "antd";
import { Route, Switch, useLocation } from "react-router-dom";
import * as DatasetMetadata from "../../navigation/DatasetMetadata";
import ColumnView from "./ColumnView";
import DatasetOverviewView from "./DatasetOverviewView";
import TableView from "./TableView";

const DatasetView = (): JSX.Element => {
  const { search } = useLocation();
  const queryParams = new URLSearchParams(search);
  const stateCode = queryParams.get("stateCode");

  return (
    <>
      <>
        {stateCode ? null : (
          <Alert
            message="Select a region to view dataset details"
            type="warning"
            showIcon
          />
        )}
      </>
      <Switch>
        <Route path={DatasetMetadata.METADATA_COLUMN_ROUTE_TEMPLATE}>
          <ColumnView stateCode={stateCode} />
        </Route>
        <Route path={DatasetMetadata.METADATA_TABLE_ROUTE_TEMPLATE}>
          <TableView stateCode={stateCode} />
        </Route>
        <Route>
          <DatasetOverviewView stateCode={stateCode} />
        </Route>
      </Switch>
    </>
  );
};

export default DatasetView;
