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
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { Pill } from "../Pill";
import { ClientProps } from "./ClientList.types";
import { StatusList } from "./ClientList.styles";
import AlertPreview from "../AlertPreview";

export const ClientStatusList: React.FC<ClientProps> = observer(
  ({ client }: ClientProps): JSX.Element => {
    const statusPills = [] as JSX.Element[];

    const notOnCaseloadAction =
      client.caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD];

    const currentlyInCustodyAction =
      client.caseUpdates[CaseUpdateActionType.CURRENTLY_IN_CUSTODY];

    if (notOnCaseloadAction) {
      statusPills.push(
        <AlertPreview key="notOnCaseload" kind="info">
          Incorrect data reported{" "}
          {moment(notOnCaseloadAction.actionTs).format("MMMM Do, YYYY")}
        </AlertPreview>
      );
    } else if (currentlyInCustodyAction) {
      statusPills.push(
        <AlertPreview key="inCustody" kind="info">
          In custody
        </AlertPreview>
      );
    }

    // if we picked up one of the above statuses, the rest of these should be ignored
    if (!statusPills.length) {
      client.activeOpportunities.forEach((opportunity) => {
        statusPills.push(
          <AlertPreview
            key={opportunity.opportunityType}
            tooltip={opportunity.tooltipText}
            {...opportunity.alertOptions.pill}
          >
            {opportunity.previewText}
          </AlertPreview>
        );
      });
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
