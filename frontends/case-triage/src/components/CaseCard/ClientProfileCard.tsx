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
  Icon,
  IconSVG,
  Link,
  spacing,
  Tab,
  TabList,
} from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import moment from "moment";
import React from "react";
import { useRootStore } from "../../stores";
import { LONG_DATE_FORMAT, remScaledPixels, titleCase } from "../../utils";
import { ClientTimeline } from "../ClientTimeline";
import TEST_IDS from "../TestIDs";
import { Caption, CloseButton } from "./CaseCard.styles";
import { CaseCardProps } from "./CaseCard.types";
import {
  ClientInfo,
  ClientName,
  ClientProfileHeading,
  ClientProfileTabPanel,
  ClientProfileTabs,
  ClientProfileWrapper,
  DetailsLabel,
  DetailsLineItem,
  DetailsPanelHeading,
  DetailsPanelSection,
  Summary,
  SummaryIcon,
  SummaryLineItem,
} from "./ClientProfileCard.styles";
import { NewItems } from "./NewItems";
import { NotInCaseloadDropdown } from "./NotInCaseloadDropdown";
import { PreferredContactMethodSelector } from "./PreferredContactMethodSelector";
import { PreferredNameInput } from "./PreferredNameInput";
import { ReceivingSSIOrDisabilityIncomeSelector } from "./ReceivingSSIOrDisabilityIncomeSelector";
import { getContactFrequencyText } from "./strings";

interface SummaryItemProps {
  icon?: typeof IconSVG[keyof typeof IconSVG];
}
const SummaryItem: React.FC<SummaryItemProps> = ({ children, icon }) => {
  return (
    <SummaryLineItem>
      {icon ? <SummaryIcon kind={icon} /> : null}
      <Summary>{children}</Summary>
    </SummaryLineItem>
  );
};

const DetailsPanelContents: React.FC<CaseCardProps> = ({ client }) => {
  const {
    policyStore,
    userStore: { canSeeExtendedProfile, canSeeClientTimeline, canSeeHomeVisit },
  } = useRootStore();

  const contactText = getContactFrequencyText(
    policyStore.findContactFrequencyForClient(client)
  );

  return (
    <>
      <DetailsPanelSection>
        <DetailsPanelHeading>Summary</DetailsPanelHeading>
        {client.supervisionStartDate ? (
          <SummaryItem icon={IconSVG.Journey}>
            Supervision started on{" "}
            {client.supervisionStartDate.format(LONG_DATE_FORMAT)} (
            {client.supervisionStartDate.from(moment().startOf("day"))})
          </SummaryItem>
        ) : null}
        {client.projectedEndDate ? (
          <SummaryItem>
            FTRD: {client.projectedEndDate.format(LONG_DATE_FORMAT)} (
            {client.projectedEndDate.from(moment().startOf("day"))})
          </SummaryItem>
        ) : null}
        {canSeeExtendedProfile && client.milestones.violationFree && (
          <SummaryItem>
            Milestone: {client.milestones.violationFree} without a violation
          </SummaryItem>
        )}

        <SummaryItem icon={IconSVG.House}>
          {client.currentAddress
            ? `Lives at ${client.currentAddress}`
            : "No address on file"}
        </SummaryItem>

        <SummaryItem icon={IconSVG.Briefcase}>
          {client.needsMet.employment
            ? `Employed at ${client.employer}`
            : "Unemployed"}{" "}
          / <ReceivingSSIOrDisabilityIncomeSelector client={client} />
        </SummaryItem>
        {canSeeExtendedProfile && client.milestones.employment && (
          <SummaryItem>
            Milestone: {client.milestones.employment} employed here
          </SummaryItem>
        )}

        <SummaryItem icon={IconSVG.Envelope}>
          {client.emailAddress ? (
            <Link href={`mailto:${client.emailAddress}`}>
              {client.emailAddress}
            </Link>
          ) : (
            "No email on file"
          )}{" "}
          /{" "}
          {client.phoneNumber ? (
            <Link href={`tel:${client.phoneNumber}`}>{client.phoneNumber}</Link>
          ) : (
            "No phone number on file"
          )}
        </SummaryItem>

        {client.hasUpcomingBirthday && (
          <SummaryItem icon={IconSVG.Gift}>
            Birthday coming up ({client.birthdate?.format("MMMM D")})
          </SummaryItem>
        )}
      </DetailsPanelSection>
      <DetailsPanelSection>
        <DetailsPanelHeading>Upcoming</DetailsPanelHeading>
        <DetailsLineItem>
          Next contact
          {client.nextFaceToFaceDate
            ? ` recommended on ${client.nextFaceToFaceDate.format(
                LONG_DATE_FORMAT
              )}`
            : ": Never"}
          <Caption style={{ marginLeft: spacing.sm }}>
            Previous:{" "}
            {client.mostRecentFaceToFaceDate?.format(LONG_DATE_FORMAT) ||
              "Never"}
          </Caption>
        </DetailsLineItem>
        {/* // TODO(#9807) remove feature flag when ready to release home visit */}
        {canSeeHomeVisit && (
          <DetailsLineItem>
            Next home visit
            {client.nextHomeVisitDate
              ? ` recommended on ${client.nextHomeVisitDate.format(
                  LONG_DATE_FORMAT
                )}`
              : ": Never"}
            <Caption style={{ marginLeft: spacing.sm }}>
              Previous:{" "}
              {client.mostRecentHomeVisitDate?.format(LONG_DATE_FORMAT) ||
                "Never"}
            </Caption>
          </DetailsLineItem>
        )}
        <DetailsLineItem>
          Next assessment
          {client.nextAssessmentDate
            ? ` due on ${client.nextAssessmentDate.format(LONG_DATE_FORMAT)}`
            : ": Not needed unless prompted by an event"}
          <Caption style={{ marginLeft: spacing.sm }}>
            Previous:{" "}
            {client.mostRecentAssessmentDate?.format(LONG_DATE_FORMAT) ||
              "Never"}
          </Caption>
        </DetailsLineItem>
        <DetailsLineItem>
          <Caption>
            {contactText && <>{contactText}.</>}
            {policyStore.policies?.supervisionPolicyReference && (
              <>
                {" "}
                <Link href={policyStore.policies.supervisionPolicyReference}>
                  Read {policyStore.getDOCName()} Policy
                </Link>
              </>
            )}
          </Caption>
        </DetailsLineItem>
      </DetailsPanelSection>
      {canSeeClientTimeline && (
        <DetailsPanelSection>
          <DetailsPanelHeading>Events</DetailsPanelHeading>
          <ClientTimeline />
        </DetailsPanelSection>
      )}
    </>
  );
};

const ClientProfileCard: React.FC<CaseCardProps> = observer(({ client }) => {
  const { clientsStore } = useRootStore();
  return (
    <ClientProfileWrapper data-testid={TEST_IDS.CLIENT_PROFILE_CARD}>
      <ClientProfileHeading className="fs-exclude">
        <ClientName>{client.name}</ClientName>

        <NotInCaseloadDropdown client={client} />

        <CloseButton onClick={() => clientsStore.view()}>
          <Icon kind={IconSVG.Close} size={remScaledPixels(14)} />
        </CloseButton>

        <ClientInfo>
          <DetailsLabel>Details:</DetailsLabel>
          {titleCase(client.supervisionType)},{" "}
          {titleCase(client.supervisionLevelText)},{" "}
          {titleCase(client.personExternalId)} <br />
          <DetailsLabel>Preferred Contact:</DetailsLabel>
          <PreferredContactMethodSelector client={client} />
          <br />
          <DetailsLabel>Preferred Name:</DetailsLabel>
          <PreferredNameInput client={client} />
        </ClientInfo>
      </ClientProfileHeading>
      <ClientProfileTabs>
        <TabList>
          <Tab>New</Tab>
          <Tab>Details</Tab>
        </TabList>
        <ClientProfileTabPanel className="fs-exclude">
          <NewItems client={client} />
        </ClientProfileTabPanel>
        <ClientProfileTabPanel className="fs-exclude">
          <DetailsPanelContents client={client} />
        </ClientProfileTabPanel>
      </ClientProfileTabs>
    </ClientProfileWrapper>
  );
});

export default ClientProfileCard;
