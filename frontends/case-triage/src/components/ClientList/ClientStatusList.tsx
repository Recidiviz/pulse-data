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
import { LONG_DATE_FORMAT, sentenceCase } from "../../utils";
import { OpportunityType } from "../../stores/OpportunityStore/Opportunity";
import { ACTION_TITLES } from "../../stores/CaseUpdatesStore/CaseUpdates";

export const ClientStatusList: React.FC<ClientProps> = observer(
  ({ client }: ClientProps): JSX.Element => {
    const statusPills = [] as JSX.Element[];

    const { pendingCaseloadRemoval } = client;

    if (pendingCaseloadRemoval) {
      const { actionType, actionTs } = pendingCaseloadRemoval;
      statusPills.push(
        <AlertPreview key={actionType} kind="info">
          {actionType === CaseUpdateActionType.CURRENTLY_IN_CUSTODY
            ? sentenceCase(ACTION_TITLES[actionType])
            : `Incorrect data reported ${moment(actionTs).format(
                LONG_DATE_FORMAT
              )}`}
        </AlertPreview>
      );
    } else {
      // these will be suppressed if removal is pending
      client.activeOpportunities.forEach((opportunity) => {
        // no pill for this opportunity
        if (opportunity.opportunityType === OpportunityType.NEW_TO_CASELOAD) {
          return;
        }

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

    const noteCount = client.activeNotes.length;

    // the "truncator" won't render if the list is empty;
    // force it to contain something invisible if we need to show a note count
    if (noteCount && statusPills.length === 0) {
      statusPills.push(<div key="placeholder" />);
    }

    return (
      <StatusList
        alwaysShowTruncator
        renderTruncator={({ hiddenItemsCount }) => {
          const additionalItemsCount = hiddenItemsCount + noteCount;

          if (additionalItemsCount) {
            return (
              <Pill kind="muted" filled>
                +{additionalItemsCount}
              </Pill>
            );
          }
          return null;
        }}
      >
        {statusPills}
      </StatusList>
    );
  }
);
