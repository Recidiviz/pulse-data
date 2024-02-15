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

import { VALIDATION_DETAIL_ROUTE_TEMPLATE } from "../../navigation/DatasetMetadata";
import { StateCodeInfo } from "../general/constants";
import ValidationDetailView from "./ValidationDetailView";
import ValidationStatusView from "./ValidationStatusView/ValidationStatusView";

const ValidationStatusOverview = (): JSX.Element => {
  const history = useHistory();
  const { search } = useLocation();
  const queryParams = new URLSearchParams(search);
  const stateCode = queryParams.get("stateCode");

  const stateCodeChange = (value: StateCodeInfo) => {
    queryParams.set("stateCode", value.code);
    history.push({ search: queryParams.toString() });
  };

  return (
    <Layout style={{ height: "100%", width: "100%" }}>
      <Switch>
        <Route path={VALIDATION_DETAIL_ROUTE_TEMPLATE}>
          <ValidationDetailView
            stateCode={stateCode}
            stateCodeChange={stateCodeChange}
          />
        </Route>
        <Route>
          <ValidationStatusView
            stateCode={stateCode}
            stateCodeChange={stateCodeChange}
          />
        </Route>
      </Switch>
    </Layout>
  );
};

export default ValidationStatusOverview;
