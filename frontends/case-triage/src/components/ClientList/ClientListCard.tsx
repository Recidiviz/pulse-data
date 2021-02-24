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
import { IconSVG, NeedState } from "@recidiviz/case-triage-components";
import { DecoratedClient } from "../../stores/ClientsStore";
import { useRootStore } from "../../stores";
import {
  CardHeader,
  ClientCard,
  ClientNeed,
  FlexCardSection,
  MainText,
  SecondaryText,
} from "./ClientList.styles";
import { titleCase } from "../../utils";
import DueDate from "../DueDate";

interface ClientProps {
  client: DecoratedClient;
  isInProgress: boolean;
}

const getNeedsMetState = (
  needsMet: DecoratedClient["needsMet"],
  need: keyof DecoratedClient["needsMet"]
): NeedState => {
  return needsMet[need] ? NeedState.MET : NeedState.NOT_MET;
};

const ClientComponent: React.FC<ClientProps> = ({
  client,
  isInProgress,
}: ClientProps) => {
  const { clientsStore } = useRootStore();
  const cardRef = React.useRef<HTMLDivElement>(null);

  const onClientCardClickHandler = () => {
    clientsStore.view(
      client,
      cardRef !== null && cardRef.current !== null
        ? cardRef.current.offsetTop
        : 0
    );
  };

  return (
    <ClientCard
      className={isInProgress ? "client-card--in-progress" : ""}
      ref={cardRef}
      onClick={onClientCardClickHandler}
    >
      <CardHeader className="fs-exclude">
        <MainText>{client.formalName}</MainText>
        <SecondaryText>
          {titleCase(client.supervisionType)},{" "}
          {titleCase(client.supervisionLevel)}
        </SecondaryText>
      </CardHeader>
      <FlexCardSection>
        <ClientNeed
          kind={IconSVG.NeedsEmployment}
          state={
            isInProgress
              ? NeedState.DISABLED
              : getNeedsMetState(client.needsMet, "employment")
          }
        />
        <ClientNeed
          kind={IconSVG.NeedsRiskAssessment}
          state={
            isInProgress
              ? NeedState.DISABLED
              : getNeedsMetState(client.needsMet, "assessment")
          }
        />
        <ClientNeed
          kind={IconSVG.NeedsContact}
          state={
            isInProgress
              ? NeedState.DISABLED
              : getNeedsMetState(client.needsMet, "faceToFaceContact")
          }
        />
      </FlexCardSection>
      <FlexCardSection>
        {isInProgress ? (
          <MainText>In progress</MainText>
        ) : (
          <DueDate date={client.nextFaceToFaceDate} />
        )}
      </FlexCardSection>
    </ClientCard>
  );
};

export default ClientComponent;
