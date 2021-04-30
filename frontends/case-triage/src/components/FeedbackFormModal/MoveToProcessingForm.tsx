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
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { DecoratedClient } from "../../stores/ClientsStore";
import InCustodyHeader from "./InCustodyHeader";
import NotOnCaseloadHeader from "./NotOnCaseloadHeader";
import BaseFeedbackForm from "./BaseForm";
import { useRootStore } from "../../stores";

interface MoveToProcessingFormProps {
  client: DecoratedClient;
  actionType: CaseUpdateActionType;

  title: string;
  onCancel: (event?: React.MouseEvent<Element, MouseEvent>) => void;
}

const MoveToProcessingForm = ({
  client,
  actionType,
  title,
  onCancel,
}: MoveToProcessingFormProps): JSX.Element => {
  const { caseUpdatesStore } = useRootStore();

  const header =
    actionType === CaseUpdateActionType.NOT_ON_CASELOAD ? (
      <NotOnCaseloadHeader />
    ) : (
      <InCustodyHeader />
    );

  return (
    <BaseFeedbackForm
      caseUpdatesStore={caseUpdatesStore}
      client={client}
      actionType={actionType}
      commentPlaceholder="Tell us more details..."
      description="After you click submit, we will move this person to the bottom of the list. Once processed, this person will be removed from your list."
      title={title}
      header={header}
      onCancel={onCancel}
    />
  );
};

export default MoveToProcessingForm;
