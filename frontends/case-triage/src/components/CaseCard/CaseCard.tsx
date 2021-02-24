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
import {
  Button,
  Card,
  H3,
  Icon,
  IconSVG,
  Modal,
} from "@recidiviz/case-triage-components";
import {
  Caption,
  CaseCardFeedback,
  CaseCardFooter,
  CaseCardSection,
  CaseCardHeading,
  CloseButton,
} from "./CaseCard.styles";
import { HEADING_HEIGHT_MAGIC_NUMBER } from "../ClientList";
import NeedsEmployment from "./NeedsEmployment";
import NeedsFaceToFaceContact from "./NeedsFaceToFaceContact";
import NeedsRiskAssessment from "./NeedsRiskAssessment";
import FeedbackForm from "../FeedbackForm";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore/CaseUpdates";
import { DecoratedClient } from "../../stores/ClientsStore";
import { titleCase } from "../../utils";
import { useRootStore } from "../../stores";

const useCardFeedback = (client: DecoratedClient) => {
  const { caseUpdatesStore } = useRootStore();
  const [feedbackModalIsOpen, setFeedbackModalIsOpen] = useState(false);

  return (
    <CaseCardFeedback>
      See something wrong?
      <br />
      <Button kind="link" onClick={() => setFeedbackModalIsOpen(true)}>
        Give us feedback.
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

  const [helpedWithEmployment, setHelpedWithEmployment] = useState(false);
  const [completedAssessment, setCompletedAssessment] = useState(false);
  const [scheduledFaceToFace, setScheduledFaceToFace] = useState(false);

  React.useEffect(() => {
    setHelpedWithEmployment(false);
    setCompletedAssessment(false);
    setScheduledFaceToFace(false);
  }, [client]);

  const actionsTaken = () => {
    const actions = [];

    if (helpedWithEmployment) {
      actions.push(CaseUpdateActionType.FOUND_EMPLOYMENT);
    }
    if (completedAssessment) {
      actions.push(CaseUpdateActionType.COMPLETED_ASSESSMENT);
    }
    if (scheduledFaceToFace) {
      actions.push(CaseUpdateActionType.SCHEDULED_FACE_TO_FACE);
    }
    return actions;
  };

  return (
    <Card
      stacked
      style={{
        marginTop:
          (clientsStore.activeClientOffset || 0) - HEADING_HEIGHT_MAGIC_NUMBER,
      }}
    >
      <CaseCardHeading className="fs-exclude">
        <H3 as="div">{client.name}</H3>

        <Caption>
          {titleCase(client.supervisionType)},{" "}
          {titleCase(client.supervisionLevel)},{" "}
          {titleCase(client.personExternalId)}
        </Caption>

        <CloseButton onClick={() => clientsStore.view()}>
          <Icon kind={IconSVG.Close} />
        </CloseButton>
      </CaseCardHeading>
      <NeedsEmployment
        client={client}
        onStatusChanged={(helped: boolean) => setHelpedWithEmployment(helped)}
        className="fs-exclude"
      />
      <NeedsRiskAssessment
        client={client}
        onStatusChanged={(completed: boolean) =>
          setCompletedAssessment(completed)
        }
        className="fs-exclude"
      />
      <NeedsFaceToFaceContact
        client={client}
        onStatusChanged={(scheduled: boolean) =>
          setScheduledFaceToFace(scheduled)
        }
        className="fs-exclude"
      />
      <CaseCardFooter>
        {useCardFeedback(client)}

        <div>
          <Button
            kind="primary"
            disabled={
              !helpedWithEmployment &&
              !completedAssessment &&
              !scheduledFaceToFace
            }
            onClick={async (e) => {
              await caseUpdatesStore.submit(client, actionsTaken(), "");
            }}
          >
            Submit
          </Button>
        </div>
      </CaseCardFooter>
    </Card>
  );
};

export default CaseCard;
