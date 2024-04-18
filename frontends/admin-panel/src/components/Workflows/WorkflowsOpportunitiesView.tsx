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

import { PageHeader, Space } from "antd";
import { observer } from "mobx-react-lite";
import { useEffect } from "react";

import { useWorkflowsStore } from "../StoreProvider";
import StateSelect from "../Utilities/StateSelect";

const WorkflowsOpportunitiesView = (): JSX.Element => {
  const store = useWorkflowsStore();

  useEffect(() => {
    if (store.hydrationState.status === "needs hydration") store.hydrate();
  }, [store]);

  const { stateCodeInfo, setStateCode, opportunities } = store;

  return (
    <>
      <PageHeader title="Workflows Configurations" />
      <Space>
        <StateSelect
          states={stateCodeInfo}
          onChange={(state) => {
            setStateCode(state.code);
          }}
        />
      </Space>
      <div>
        {opportunities &&
          `This state has ${opportunities.length} opportunities.`}
      </div>
    </>
  );
};

export default observer(WorkflowsOpportunitiesView);
