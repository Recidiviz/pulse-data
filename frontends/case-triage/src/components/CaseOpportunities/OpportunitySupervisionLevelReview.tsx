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
import { IconSVG, Need, NeedState } from "@recidiviz/design-system";
import * as React from "react";
import { observer } from "mobx-react-lite";
import {
  Caption,
  CaseCardBody,
  CaseCardInfo,
} from "../CaseCard/CaseCard.styles";
import { Client } from "../../stores/ClientsStore/Client";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { NeedsActionFlow } from "../NeedsActionFlow/NeedsActionFlow";
import { titleCase } from "../../utils";
import { Opportunity } from "../../stores/OpportunityStore";
import { PolicyLink } from "./PolicyLink";

interface OpportunitySupervisionLevelReviewProps {
  className: string;
  client: Client;
  opportunity: Opportunity;
}

const OpportunitySupervisionLevelReview: React.FC<OpportunitySupervisionLevelReviewProps> =
  ({
    className,
    client,
    opportunity,
  }: OpportunitySupervisionLevelReviewProps) => {
    return (
      <CaseCardBody className={className}>
        <Need kind={IconSVG.Star} state={NeedState.NOT_MET} />
        <CaseCardInfo>
          <strong>Supervision Downgrade Recommended</strong>
          <br />
          <Caption>
            {titleCase(client.fullName.given_names)} was assessed with score{" "}
            {client.assessmentScore} on{" "}
            {client.mostRecentAssessmentDate?.format("LL")}.{" "}
            {titleCase(client.possessivePronoun)} risk level is recorded as{" "}
            <strong>{client.supervisionLevelText}</strong> but should be{" "}
            <strong>{client.riskLevelLabel}</strong> according to{" "}
            <PolicyLink opportunity={opportunity} />.
          </Caption>

          <NeedsActionFlow
            actionable
            client={client}
            opportunity={opportunity}
            resolve={CaseUpdateActionType.DOWNGRADE_INITIATED}
            dismiss={CaseUpdateActionType.INCORRECT_SUPERVISION_LEVEL_DATA}
          />
        </CaseCardInfo>
      </CaseCardBody>
    );
  };

export default observer(OpportunitySupervisionLevelReview);
