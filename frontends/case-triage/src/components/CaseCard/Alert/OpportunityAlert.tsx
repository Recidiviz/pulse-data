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
  Button,
  DropdownMenuItem,
  DropdownMenuLabel,
  Icon,
  Link,
  useToasts,
} from "@recidiviz/design-system";
import assertNever from "assert-never";
import { paramCase } from "param-case";
import React from "react";
import { observer } from "mobx-react-lite";
import { flowResult } from "mobx";
import { useRootStore } from "../../../stores";
import {
  ACTION_TITLES,
  CaseUpdateActionType,
  CASE_UPDATE_OPPORTUNITY_ASSOCIATION,
} from "../../../stores/CaseUpdatesStore/CaseUpdates";
import { Client } from "../../../stores/ClientsStore";
import { Opportunity } from "../../../stores/OpportunityStore";
import { OpportunityType } from "../../../stores/OpportunityStore/Opportunity";
import { parseContactFrequency } from "../../../stores/PolicyStore/Policy";
import { LONG_DATE_FORMAT, titleCase } from "../../../utils";
import FeedbackFormModal from "../../FeedbackFormModal";
import { ReminderMenuItems } from "../../NeedsActionFlow/ReminderMenuItems";
import { PolicyLink } from "../../CaseOpportunities/PolicyLink";
import { Alert } from "./Alert";
import { UndoContents } from "./Alert.styles";
import { NewToCaseloadPolicy } from "./NewToCaseloadPolicy";

const titleForActionType = (actionType: CaseUpdateActionType): string => {
  switch (actionType) {
    case CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA:
    case CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA:
    case CaseUpdateActionType.INCORRECT_CONTACT_DATA:
      return `${ACTION_TITLES[actionType]}`;
    default:
      return `Report ${ACTION_TITLES[actionType].toLowerCase()}`;
  }
};

const AlertText = ({ client, opportunity }: OpportunityReviewProps) => {
  const { policyStore } = useRootStore();
  const { omsName } = policyStore;

  switch (opportunity.opportunityType) {
    case OpportunityType.OVERDUE_DOWNGRADE:
      return (
        <>
          {client.givenName} was assessed with score {client.assessmentScore} on{" "}
          {client.mostRecentAssessmentDate?.format("LL")}.{" "}
          {titleCase(client.possessivePronoun)} risk level is recorded as{" "}
          <strong>{client.supervisionLevelText}</strong> but should be{" "}
          <strong>{client.riskLevelLabel}</strong> according to{" "}
          <PolicyLink opportunity={opportunity} />. Correct{" "}
          {client.possessivePronoun} supervision level in {omsName}.
        </>
      );
    case OpportunityType.NEW_TO_CASELOAD:
      return <NewToCaseloadPolicy />;
    case OpportunityType.EMPLOYMENT:
      return (
        <>
          Unemployed according to {omsName}.<br />
          <Link
            href={`https://www.recidiviz.org/app/resources/${paramCase(
              opportunity.stateCode
            )}`}
            rel="noopener noreferrer"
            target="_blank"
          >
            Click to view resources.
          </Link>
        </>
      );
    case OpportunityType.ASSESSMENT: {
      if (client.mostRecentAssessmentDate) {
        return (
          <>
            Last assessed on{" "}
            {client.mostRecentAssessmentDate.format(LONG_DATE_FORMAT)}
            <br />
            Score: {client.assessmentScore}, {client.assessmentScoreDetails}
          </>
        );
      }
      return <>A risk assessment has never been completed.</>;
    }
    case OpportunityType.CONTACT: {
      let contactPolicyText;
      const contactFrequency =
        policyStore.findContactFrequencyForClient(client);
      if (contactFrequency) {
        const [contacts, days] = parseContactFrequency(contactFrequency);
        contactPolicyText = (
          <>
            {contacts} needed every {days} (view{" "}
            <PolicyLink opportunity={opportunity} />)
          </>
        );
      }
      return (
        <>
          {client.currentAddress || "No address on file"} <br />
          {client.mostRecentFaceToFaceDate
            ? `Last contacted on ${client.mostRecentFaceToFaceDate.format(
                LONG_DATE_FORMAT
              )}`
            : "No contact on file."}
          {contactPolicyText && (
            <>
              <br />
              {contactPolicyText}
            </>
          )}
        </>
      );
    }
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
      {opportunity.opportunityType === OpportunityType.EMPLOYMENT && (
        <DropdownMenuItem
          onClick={() => {
            client.updateReceivingSSIOrDisabilityIncome(true);
          }}
        >
          Mark as SSI/Disability
        </DropdownMenuItem>
      )}
      <DropdownMenuItem
        onClick={() => {
          setFeedbackModalIsOpen(true);
        }}
      >
        {titleForActionType(incorrectDataActionType)}
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

export const OpportunityAlert = observer(
  ({ client, opportunity }: OpportunityReviewProps): JSX.Element => {
    const { opportunityStore } = useRootStore();
    const { addToast, removeToast } = useToasts();

    const toastId = `${opportunity.deferralId}-successful`;

    return (
      <Alert
        bullet={<Icon size={16} {...opportunity.alertOptions.icon} />}
        title={opportunity.title}
        body={<AlertText {...{ client, opportunity }} />}
        menuItems={
          <>
            <ReminderMenuItems
              onDeferred={async (deferUntil) => {
                await flowResult(
                  opportunityStore.createOpportunityDeferral(
                    opportunity,
                    deferUntil
                  )
                );

                const snoozeDuration = deferUntil.fromNow(true);
                addToast(
                  <UndoContents>
                    Snoozed for {snoozeDuration}.{" "}
                    <Button
                      kind="secondary"
                      onClick={() => {
                        opportunityStore.deleteOpportunityDeferral(opportunity);
                        removeToast(toastId);
                      }}
                      shape="block"
                    >
                      Undo
                    </Button>
                  </UndoContents>,
                  {
                    appearance: "info",
                    id: toastId,
                  }
                );
              }}
            />
            <DropdownMenuLabel>Other Actions</DropdownMenuLabel>
            <MenuActions {...{ client, opportunity }} />
          </>
        }
      />
    );
  }
);
