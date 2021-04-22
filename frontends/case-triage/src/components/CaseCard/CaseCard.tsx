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
import { useState } from "react";
import { Button, H3, Icon, IconSVG, Modal } from "@recidiviz/design-system";
import { autorun } from "mobx";
import {
  Caption,
  CaseCard as CaseCardComponent,
  CaseCardFeedback,
  CaseCardFooter,
  CaseCardHeading,
  ClientNameRow,
  CloseButton,
} from "./CaseCard.styles";
import NeedsEmployment from "./NeedsEmployment";
import NeedsFaceToFaceContact from "./NeedsFaceToFaceContact";
import NeedsRiskAssessment from "./NeedsRiskAssessment";
import FeedbackForm from "../FeedbackForm";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore/CaseUpdates";
import { DecoratedClient } from "../../stores/ClientsStore";
import { titleCase } from "../../utils";
import { useRootStore } from "../../stores";
import NotInCaseload from "./NotInCaseload";

const useCardFeedback = (client: DecoratedClient) => {
  const { caseUpdatesStore } = useRootStore();
  const [feedbackModalIsOpen, setFeedbackModalIsOpen] = useState(false);

  return (
    <CaseCardFeedback>
      See something wrong?
      <br />
      <Button kind="link" onClick={() => setFeedbackModalIsOpen(true)}>
        Submit a correction
      </Button>
      <Modal
        isOpen={feedbackModalIsOpen}
        onRequestClose={() => setFeedbackModalIsOpen(false)}
      >
        <FeedbackForm
          caseUpdatesStore={caseUpdatesStore}
          client={client}
          onCancel={() => setFeedbackModalIsOpen(false)}
        />
      </Modal>
    </CaseCardFeedback>
  );
};

export interface CaseCardProps {
  client: DecoratedClient;
}

const CaseCard: React.FC<CaseCardProps> = ({ client }: CaseCardProps) => {
  const { caseUpdatesStore, clientsStore } = useRootStore();

  const [marginTop, setMarginTop] = useState(clientsStore.activeClientOffset);

  React.useEffect(() =>
    autorun(() => {
      setMarginTop(clientsStore.activeClientOffset);
    })
  );

  const recordEvent = (
    eventType: CaseUpdateActionType,
    completedAction: boolean
  ): void => {
    if (completedAction) {
      caseUpdatesStore.recordAction(client, eventType);
    } else {
      const updateId = client.caseUpdates[eventType]?.updateId;
      if (updateId) {
        caseUpdatesStore.removeAction(client, updateId, eventType);
      }
    }
  };

  return (
    <CaseCardComponent stacked style={{ marginTop }}>
      <CaseCardHeading className="fs-exclude">
        <ClientNameRow>
          <H3 as="div">{client.name}</H3>
          <CloseButton onClick={() => clientsStore.view()}>
            <Icon kind={IconSVG.Close} />
          </CloseButton>
        </ClientNameRow>
        <Caption>
          {titleCase(client.supervisionType)},{" "}
          {titleCase(client.supervisionLevelText)},{" "}
          {titleCase(client.personExternalId)}
        </Caption>
      </CaseCardHeading>
      {client.caseUpdates[CaseUpdateActionType.NOT_ON_CASELOAD] ? (
        <NotInCaseload
          client={client}
          onUndo={() => {
            const actionType = CaseUpdateActionType.NOT_ON_CASELOAD;
            const updateId = client.caseUpdates[actionType]?.updateId;
            caseUpdatesStore.removeAction(client, updateId, actionType);
          }}
          className="fs-exclude"
        />
      ) : null}
      <NeedsEmployment
        client={client}
        onStatusChanged={(helped: boolean) =>
          recordEvent(CaseUpdateActionType.FOUND_EMPLOYMENT, helped)
        }
        className="fs-exclude"
      />
      <NeedsRiskAssessment
        client={client}
        onStatusChanged={(completed: boolean) =>
          recordEvent(CaseUpdateActionType.COMPLETED_ASSESSMENT, completed)
        }
        className="fs-exclude"
      />
      <NeedsFaceToFaceContact
        client={client}
        onStatusChanged={(scheduled: boolean) =>
          recordEvent(CaseUpdateActionType.SCHEDULED_FACE_TO_FACE, scheduled)
        }
        className="fs-exclude"
      />
      <CaseCardFooter>{useCardFeedback(client)}</CaseCardFooter>
    </CaseCardComponent>
  );
};

export default CaseCard;
