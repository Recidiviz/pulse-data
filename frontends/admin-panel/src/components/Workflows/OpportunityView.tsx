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

import { Button, Form } from "antd";
import { observer } from "mobx-react-lite";
import { useHistory } from "react-router";

import OpportunityPresenter from "../../WorkflowsStore/presenters/OpportunityPresenter";
import { useWorkflowsStore } from "../StoreProvider";
import HydrationWrapper from "./HydrationWrapper";
import OpportunityConfigurationsTable from "./OpportunityConfigurationsTable";
import { buildRoute } from "./utils";

const OpportunitySettings = ({
  presenter,
}: {
  presenter: OpportunityPresenter;
}) => {
  const opportunity = presenter?.selectedOpportunity;
  if (!opportunity) return <div />;

  return (
    <Form>
      <Form.Item label="System">
        <span className="ant-form-text">{opportunity.systemType}</span>
      </Form.Item>
      <Form.Item label="Gating Feature Variant">
        <span className="ant-form-text">
          {opportunity.gatingFeatureVariant ?? <i>None</i>}
        </span>
      </Form.Item>
      <Form.Item label="URL Section">
        <span className="ant-form-text">{opportunity.urlSection}</span>
      </Form.Item>
      <Form.Item label="Completion Event">
        <span className="ant-form-text">{opportunity.completionEvent}</span>
      </Form.Item>{" "}
      <Form.Item label="Experiment ID">
        <span className="ant-form-text">{opportunity.experimentId}</span>
      </Form.Item>
      <Form.Item label="Last Updated">
        <span className="ant-form-text">
          {opportunity.lastUpdatedAt.toLocaleString()} by{" "}
          {opportunity.lastUpdatedBy}
        </span>
      </Form.Item>
    </Form>
  );
};

const OpportunityView = (): JSX.Element => {
  const {
    opportunityPresenter,
    opportunityConfigurationPresenter,
    stateCode,
    selectedOpportunityType,
  } = useWorkflowsStore();

  const history = useHistory();

  return (
    <>
      <h2>Settings</h2>
      <HydrationWrapper
        presenter={opportunityPresenter}
        component={OpportunitySettings}
      />
      <h2>Configurations</h2>
      <Button
        onClick={() =>
          history.push(
            buildRoute(stateCode ?? "", selectedOpportunityType, "new")
          )
        }
      >
        New Configuration
      </Button>
      <HydrationWrapper
        presenter={opportunityConfigurationPresenter}
        component={OpportunityConfigurationsTable}
      />
    </>
  );
};

export default observer(OpportunityView);
