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
import {
  Button,
  ButtonKind,
  Card,
  H3,
} from "@recidiviz/case-triage-components";
import {
  Caption,
  CaseCardFeedback,
  CaseCardFooter,
  CaseCardSection,
} from "./CaseCard.styles";
import { HEADING_HEIGHT_MAGIC_NUMBER } from "../ClientList";
import NeedsEmployment from "./NeedsEmployment";
import NeedsFaceToFaceContact from "./NeedsFaceToFaceContact";
import NeedsRiskAssessment from "./NeedsRiskAssessment";
import { DecoratedClient } from "../../stores/ClientsStore/Client";
import { titleCase } from "../../utils";
import { useRootStore } from "../../stores";

export interface CaseCardProps {
  client: DecoratedClient;
}

const onFeedback = () => {
  return false;
};

const onSubmit = () => {
  return false;
};

const CaseCard: React.FC<CaseCardProps> = ({ client }: CaseCardProps) => {
  const { clientsStore } = useRootStore();

  return (
    <Card
      stacked
      style={{
        marginTop:
          (clientsStore.activeClientOffset || 0) - HEADING_HEIGHT_MAGIC_NUMBER,
      }}
    >
      <CaseCardSection className="fs-exclude">
        <H3 as="div">{client.name}</H3>

        <Caption>
          {titleCase(client.supervisionType)},{" "}
          {titleCase(client.supervisionLevel)},{" "}
          {titleCase(client.personExternalId)}
        </Caption>
      </CaseCardSection>
      <NeedsEmployment client={client} className="fs-exclude" />
      <NeedsRiskAssessment client={client} className="fs-exclude" />
      <NeedsFaceToFaceContact client={client} className="fs-exclude" />
      <CaseCardFooter>
        <CaseCardFeedback>
          See something wrong?
          <br />
          <Button kind={ButtonKind.link} onClick={onFeedback}>
            Give us feedback.
          </Button>
        </CaseCardFeedback>
        <div>
          <Button kind={ButtonKind.primary} onClick={onSubmit}>
            Submit
          </Button>
        </div>
      </CaseCardFooter>
    </Card>
  );
};

export default CaseCard;
