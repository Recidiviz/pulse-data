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
import { Layout } from "antd";
import { Route, Switch, useHistory, useLocation } from "react-router-dom";
import {
  INGEST_ACTIONS_PRIMARY_ROUTE,
  INGEST_ACTIONS_ROUTE,
  INGEST_ACTIONS_WITH_STATE_CODE_ROUTE,
} from "../../navigation/IngestOperations";
import IngestInstanceCurrentStatusSummary from "./IngestInstanceCurrentStatusSummary";
import IngestStateSpecificMetadata from "./IngestStateSpecificMetadata";

const IngestOperationsView = (): JSX.Element => {
  const history = useHistory();

  const location = useLocation();
  const queryString = location.search;
  const params = new URLSearchParams(queryString);

  const initialStateCode = params.get("stateCode");
  if (initialStateCode) {
    history.push(
      INGEST_ACTIONS_PRIMARY_ROUTE.replace(":stateCode", initialStateCode)
    );
  }

  return (
    <Layout style={{ height: "100%", width: "100%" }}>
      <Switch>
        <Route
          path={INGEST_ACTIONS_WITH_STATE_CODE_ROUTE}
          component={IngestStateSpecificMetadata}
        />
        <Route
          path={INGEST_ACTIONS_ROUTE}
          component={IngestInstanceCurrentStatusSummary}
        />
      </Switch>
    </Layout>
  );
};
export default IngestOperationsView;
