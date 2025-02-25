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

import { Breadcrumb, PageHeader } from "antd";
import { observer } from "mobx-react-lite";
import { useCallback, useEffect } from "react";
import { useHistory, useParams } from "react-router-dom";

import { useWorkflowsStore } from "../StoreProvider";
import StateSelect from "../Utilities/StateSelect";
import { buildRoute } from "./utils";

function backRoute(
  stateCode: string,
  opportunityType?: string,
  configId?: string
) {
  if (configId) return buildRoute(stateCode, opportunityType);
  if (opportunityType) return buildRoute(stateCode);
  return undefined;
}

const OpportunitiesHeaderView = (): JSX.Element => {
  const store = useWorkflowsStore();
  const {
    stateCodeInfo,
    setStateCode,
    setSelectedOpportunityType,
    setSelectedOpportunityConfigurationId,
  } = store;

  const { stateCode, opportunityType, configId } = useParams<{
    stateCode: string;
    opportunityType?: string;
    configId?: string;
  }>();
  const history = useHistory();

  const stateRedir = useCallback(
    (code: string) => {
      history.push(buildRoute(code, opportunityType, configId));
    },
    [history, opportunityType, configId]
  );

  useEffect(() => {
    if (store.hydrationState.status === "needs hydration") store.hydrate();
  }, [store]);

  useEffect(() => {
    if (stateCodeInfo) {
      if (stateCodeInfo.find(({ code }) => code === stateCode)) {
        setStateCode(stateCode);
      } else {
        stateRedir(stateCodeInfo[0].code);
      }
    }
  }, [setStateCode, stateCode, stateCodeInfo, stateRedir]);

  useEffect(() => {
    setSelectedOpportunityType(opportunityType);
  }, [setSelectedOpportunityType, opportunityType]);

  useEffect(() => {
    let configNum;
    const fromId = new URLSearchParams(document.location.search).get("from");
    if (configId === "new" && fromId) {
      configNum = parseInt(fromId);
    } else if (configId) {
      configNum = parseInt(configId);
    }
    if (Number.isNaN(configNum)) configNum = undefined;
    setSelectedOpportunityConfigurationId(configNum);
  }, [setSelectedOpportunityConfigurationId, configId]);

  const breadcrumbs = [];
  let title = "Workflows Opportunities";

  if (opportunityType) {
    breadcrumbs.push(title);
    title = opportunityType;
  }

  if (configId) {
    breadcrumbs.push(title);
    if (configId === "new") {
      title = "New Configuration";
    } else {
      title =
        store.opportunityConfigurationPresenter
          ?.selectedOpportunityConfiguration?.displayName ?? "";
    }
  }

  return (
    <>
      {stateCodeInfo && (
        <StateSelect
          initialValue={stateCode}
          states={stateCodeInfo}
          disabled={!!opportunityType}
          onChange={(state) => {
            stateRedir(state.code);
          }}
        />
      )}
      <PageHeader
        title={title}
        breadcrumb={
          <Breadcrumb>
            {breadcrumbs.map((b) => (
              <Breadcrumb.Item key={b}>{b}</Breadcrumb.Item>
            ))}
          </Breadcrumb>
        }
        onBack={
          opportunityType
            ? () =>
                history.push(backRoute(stateCode, opportunityType, configId))
            : undefined
        }
      />
    </>
  );
};

export default observer(OpportunitiesHeaderView);
