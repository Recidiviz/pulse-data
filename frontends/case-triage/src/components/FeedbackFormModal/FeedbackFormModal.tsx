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
import { Modal } from "@recidiviz/design-system";
import React from "react";
import styled from "styled-components/macro";
import {
  CaseUpdateActionType,
  NotInCaseloadActions,
} from "../../stores/CaseUpdatesStore";
import { DecoratedClient } from "../../stores/ClientsStore";
import IncorrectDataForm from "./IncorrectDataForm";
import MoveToProcessingForm from "./MoveToProcessingForm";

const UnpaddedModal = styled(Modal)`
  .ReactModal__Content {
    padding: 0 !important;
  }
`;

interface FeedbackFormModalProps {
  actionType: CaseUpdateActionType;
  client: DecoratedClient;
  isOpen: boolean;
  onRequestClose: () => void;
}

const titleForActionType = (actionType: CaseUpdateActionType): string => {
  switch (actionType) {
    case CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA:
      return "Incorrect assessment data";
    case CaseUpdateActionType.INCORRECT_EMPLOYMENT_DATA:
      return "Incorrect employment data";
    case CaseUpdateActionType.INCORRECT_CONTACT_DATA:
      return "Incorrect contact data";
    case CaseUpdateActionType.NOT_ON_CASELOAD:
      return "Not on Caseload";
    case CaseUpdateActionType.CURRENTLY_IN_CUSTODY:
      return "In custody";
    default:
      return "Incorrect data";
  }
};

const FeedbackFormModal = ({
  actionType,
  client,
  isOpen,
  onRequestClose,
}: FeedbackFormModalProps): JSX.Element => {
  const FormType =
    NotInCaseloadActions.indexOf(actionType) !== -1
      ? MoveToProcessingForm
      : IncorrectDataForm;
  return (
    <UnpaddedModal isOpen={isOpen} onRequestClose={onRequestClose}>
      <FormType
        client={client}
        title={titleForActionType(actionType)}
        actionType={actionType}
        onCancel={onRequestClose}
      />
    </UnpaddedModal>
  );
};

export default FeedbackFormModal;
