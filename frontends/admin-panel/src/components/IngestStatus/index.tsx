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
import { Layout } from "antd";
import { Route, Switch } from "react-router-dom";

import {
  INGEST_DATAFLOW_ROUTE,
  INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE,
} from "../../navigation/IngestOperations";
import IngestDataflowCurrentStatusSummary from "./IngestDataflowCurrentStatusSummary";
import IngestDataflowStateSpecificMetadata from "./IngestDataflowStateSpecificMetadata";

const IngestStatusView = (): JSX.Element => {
  return (
    <Layout style={{ height: "100%", width: "100%" }}>
      <Switch>
        <Route
          path={INGEST_DATAFLOW_WITH_STATE_CODE_ROUTE}
          component={IngestDataflowStateSpecificMetadata}
        />
        <Route
          path={INGEST_DATAFLOW_ROUTE}
          component={IngestDataflowCurrentStatusSummary}
        />
      </Switch>
    </Layout>
  );
};
export default IngestStatusView;
