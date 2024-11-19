// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { Button } from "antd";
import { observer } from "mobx-react-lite";
import { useHistory } from "react-router-dom";

import { useWorkflowsStore } from "../StoreProvider";
import HydrationWrapper from "./HydrationWrapper";
import OpportunityConfigurationsTable from "./OpportunityConfigurationsTable";
import OpportunitySettings from "./OpportunitySettings";
import { buildRoute } from "./utils";

const OpportunityView = (): JSX.Element => {
  const {
    opportunityPresenter,
    opportunityConfigurationPresenter,
    stateCode,
    selectedOpportunityType,
  } = useWorkflowsStore();

  const history = useHistory();

  const isOpportunityProvisioned =
    !!opportunityPresenter?.selectedOpportunity?.lastUpdatedAt;

  return (
    <>
      <HydrationWrapper
        presenter={opportunityPresenter}
        component={OpportunitySettings}
      />
      <h2>Configurations</h2>
      {isOpportunityProvisioned ? (
        <>
          <Button
            onClick={() =>
              history.push(
                buildRoute(stateCode ?? "", selectedOpportunityType, "new")
              )
            }
            disabled={!isOpportunityProvisioned}
          >
            New Configuration
          </Button>
          <HydrationWrapper
            presenter={opportunityConfigurationPresenter}
            component={OpportunityConfigurationsTable}
          />
        </>
      ) : (
        "Set up the opportunity above before creating a configuration."
      )}
    </>
  );
};

export default observer(OpportunityView);
