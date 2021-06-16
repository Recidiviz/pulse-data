// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import moment from "moment";
import { observer } from "mobx-react-lite";
import assertNever from "assert-never";
import { useRootStore } from "../../stores";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { OPPORTUNITY_TITLES } from "../../stores/OpportunityStore/Opportunity";
import { Pill } from "../Pill";
import { ClientProps } from "./ClientList.types";
import { StatusList } from "./ClientList.styles";
import { useDueDateStatus } from "../DueDate/useDueDateStatus";
import Tooltip from "../Tooltip";

export const ClientStatusList: React.FC<ClientProps> = observer(
  ({ client }: ClientProps): JSX.Element => {
    const { opportunityStore } = useRootStore();

    const statusPills = [] as JSX.Element[];

    const notOnCaseloadAction =
      client.caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD];

    const currentlyInCustodyAction =
      client.caseUpdates[CaseUpdateActionType.CURRENTLY_IN_CUSTODY];

    if (notOnCaseloadAction) {
      statusPills.push(
        <Pill key="notOnCaseload" kind="info" filled={false}>
          Incorrect data reported{" "}
          {moment(notOnCaseloadAction.actionTs).format("MMMM Do, YYYY")}
        </Pill>
      );
    } else if (currentlyInCustodyAction) {
      statusPills.push(
        <Pill key="inCustody" kind="info" filled={false}>
          In custody
        </Pill>
      );
    }

    // if we picked up one of the above statuses, the rest of these should be ignored
    if (!statusPills.length) {
      // top opportunities appear first
      const topOpp = opportunityStore.getTopOpportunityForClient(
        client.personExternalId
      );

      if (topOpp && !topOpp.deferredUntil) {
        statusPills.push(
          <Pill key="topOpp" filled={false} kind="highlight">
            {OPPORTUNITY_TITLES[topOpp.opportunityType]}
          </Pill>
        );
      }

      // other needs follow in order of importance
      if (!client.needsMet.employment) {
        statusPills.push(
          <Pill key="employment" filled={false} kind="warn">
            Unemployed
          </Pill>
        );
      }

      // TODO(#7801) add alert for risk assessment upcoming
      if (!client.needsMet.assessment) {
        statusPills.push(
          <Pill key="riskAssessment" filled={false} kind="error">
            Risk assessment overdue
          </Pill>
        );
      }

      const contactDue = useDueDateStatus({ date: client.nextContactDate });
      if (contactDue) {
        let statusPill: JSX.Element;

        switch (contactDue.status) {
          case "Past":
            statusPill = (
              <Pill filled={false} kind="error">
                Contact overdue
              </Pill>
            );
            break;
          case "Today":
          case "Future":
            // TODO(#7801) more limited time horizon for "upcoming"?
            statusPill = (
              <Pill filled={false} kind="warn">
                Contact upcoming
              </Pill>
            );
            break;
          default:
            assertNever(contactDue.status);
        }

        statusPills.push(
          <Tooltip
            key="nextContact"
            title={`Face to Face Contact recommended ${contactDue.timeDifference}`}
          >
            {statusPill}
          </Tooltip>
        );
      }
    }

    return (
      <StatusList
        renderTruncator={({ hiddenItemsCount }) => {
          return (
            <Pill kind="muted" filled>
              +{hiddenItemsCount}
            </Pill>
          );
        }}
      >
        {statusPills}
      </StatusList>
    );
  }
);
