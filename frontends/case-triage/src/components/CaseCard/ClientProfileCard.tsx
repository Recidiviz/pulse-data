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
import { Icon, IconSVG, spacing, Tab, TabList } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import moment from "moment";
import React from "react";
import { useRootStore } from "../../stores";
import { titleCase } from "../../utils";
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
    <DetailsLineItem>
      {icon ? <SummaryIcon kind={icon} /> : null}
      <Summary>{children}</Summary>
    </DetailsLineItem>
  );
};

const DetailsPanelContents: React.FC<CaseCardProps> = ({ client }) => {
  const { policyStore } = useRootStore();

  return (
    <>
      <DetailsPanelSection>
        <DetailsPanelHeading>Summary</DetailsPanelHeading>
        {client.supervisionStartDate ? (
          <SummaryItem icon={IconSVG.Journey}>
            Supervision started on{" "}
            {client.supervisionStartDate.format("MMMM Do, YYYY")} (
            {client.supervisionStartDate.from(moment().startOf("day"))})
          </SummaryItem>
        ) : null}

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

        <SummaryItem icon={IconSVG.Envelope}>
          {client.emailAddress || "No email on file"} /{" "}
          {client.phoneNumber || "No phone number on file"}
        </SummaryItem>
      </DetailsPanelSection>
      <DetailsPanelSection>
        <DetailsPanelHeading>Upcoming</DetailsPanelHeading>
        <DetailsLineItem>
          Next Contact:{" "}
          {client.nextFaceToFaceDate?.format("MM/DD/YYYY") || "Never"}
          <Caption style={{ marginLeft: spacing.sm }}>
            Previous:{" "}
            {client.mostRecentFaceToFaceDate?.format("MM/DD/YYYY") || "Never"}
          </Caption>
        </DetailsLineItem>
        <DetailsLineItem>
          Next Assessment:{" "}
          {client.nextAssessmentDate?.format("MM/DD/YYYY") || "Unknown"}
          <Caption style={{ marginLeft: spacing.sm }}>
            Previous:{" "}
            {client.mostRecentAssessmentDate?.format("MM/DD/YYYY") || "Never"}
          </Caption>
        </DetailsLineItem>
        <DetailsLineItem>
          <Caption>
            {getContactFrequencyText(
              policyStore.findContactFrequencyForClient(client),
              "contact"
            )}

            {/* TODO(#8056): Add policy link */}
          </Caption>
        </DetailsLineItem>
      </DetailsPanelSection>
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
          <Icon kind={IconSVG.Close} />
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
