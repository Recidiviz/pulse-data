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

import {
  Dropdown,
  DropdownMenu,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownToggle,
  Icon,
} from "@recidiviz/design-system";
import assertNever from "assert-never";
import React from "react";
import { useRootStore } from "../../stores";
import {
  ACTION_TITLES,
  CASE_UPDATE_OPPORTUNITY_ASSOCIATION,
} from "../../stores/CaseUpdatesStore/CaseUpdates";
import { Client } from "../../stores/ClientsStore";
import { Opportunity } from "../../stores/OpportunityStore";
import { OpportunityType } from "../../stores/OpportunityStore/Opportunity";
import { titleCase } from "../../utils";
import { ActionRow } from "../CaseCard/ActionRow";
import { Caption } from "../CaseCard/CaseCard.styles";
import FeedbackFormModal from "../FeedbackFormModal";
import { ReminderMenuItems } from "../NeedsActionFlow/ReminderMenuItems";
import { ReviewContents } from "./OpportunityReview.styles";
import { PolicyLink } from "./PolicyLink";

const AlertText = ({ client, opportunity }: OpportunityReviewProps) => {
  const {
    policyStore: { omsName },
  } = useRootStore();

  switch (opportunity.opportunityType) {
    case OpportunityType.OVERDUE_DOWNGRADE:
      return (
        <>
          {titleCase(client.fullName.given_names)} was assessed with score{" "}
          {client.assessmentScore} on{" "}
          {client.mostRecentAssessmentDate?.format("LL")}.{" "}
          {titleCase(client.possessivePronoun)} risk level is recorded as{" "}
          <strong>{client.supervisionLevelText}</strong> but should be{" "}
          <strong>{client.riskLevelLabel}</strong> according to{" "}
          <PolicyLink opportunity={opportunity} />. Correct{" "}
          {client.possessivePronoun} supervision level in {omsName}.
        </>
      );
    case OpportunityType.EMPLOYMENT: // TODO(#8191)
    case OpportunityType.ASSESSMENT: // TODO(#8194)
    case OpportunityType.CONTACT: // TODO(#8196)
      return <></>;
    default:
      assertNever(opportunity.opportunityType);
  }
};

const MenuActions = ({ client, opportunity }: OpportunityReviewProps) => {
  const incorrectDataActionType =
    CASE_UPDATE_OPPORTUNITY_ASSOCIATION[opportunity.opportunityType][1];
  const [feedbackModalIsOpen, setFeedbackModalIsOpen] = React.useState(false);

  return (
    <>
      <DropdownMenuItem
        onClick={() => {
          setFeedbackModalIsOpen(true);
        }}
      >
        Report {ACTION_TITLES[incorrectDataActionType].toLowerCase()}
      </DropdownMenuItem>
      <FeedbackFormModal
        isOpen={feedbackModalIsOpen}
        onRequestClose={() => setFeedbackModalIsOpen(false)}
        actionType={incorrectDataActionType}
        client={client}
      />
    </>
  );
};

type OpportunityReviewProps = {
  client: Client;
  opportunity: Opportunity;
};

export const OpportunityReview = ({
  client,
  opportunity,
}: OpportunityReviewProps): JSX.Element => {
  const { opportunityStore } = useRootStore();

  return (
    <ActionRow bullet={<Icon size={16} {...opportunity.alertOptions.icon} />}>
      <ReviewContents>
        <div>
          <strong>{opportunity.title}</strong>
        </div>
        <Caption>
          <AlertText {...{ client, opportunity }} />
        </Caption>
      </ReviewContents>
      <Dropdown>
        <DropdownToggle kind="borderless" icon="TripleDot" shape="block" />
        <DropdownMenu alignment="right">
          <ReminderMenuItems
            onDeferred={(deferUntil) => {
              opportunityStore.createOpportunityDeferral(
                opportunity,
                deferUntil
              );
            }}
          />
          <DropdownMenuLabel>Other Actions</DropdownMenuLabel>
          <MenuActions {...{ client, opportunity }} />
        </DropdownMenu>
      </Dropdown>
    </ActionRow>
  );
};
