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
  Need,
  NeedState,
  palette,
} from "@recidiviz/design-system";
import * as React from "react";
import {
  ButtonContainer,
  Caption,
  CaseCardBody,
  CaseCardInfo,
} from "./CaseCard.styles";
import { NeedsCheckboxButton } from "./CaseCardButtons";
import { DecoratedClient } from "../../stores/ClientsStore/Client";
import { useRootStore } from "../../stores";
import { SupervisionContactFrequency } from "../../stores/PolicyStore/Policy";
import {
  CaseUpdateActionType,
  CaseUpdateStatus,
} from "../../stores/CaseUpdatesStore";

interface NeedsFaceToFaceContactProps {
  className: string;
  client: DecoratedClient;

  onStatusChanged: (helped: boolean) => void;
}

const getLastContactedText = (client: DecoratedClient) => {
  const { mostRecentFaceToFaceDate } = client;

  if (mostRecentFaceToFaceDate) {
    return `Last contacted on ${mostRecentFaceToFaceDate.format(
      "MMMM Do, YYYY"
    )}`;
  }
  return `Assumed no contact from CIS.`;
};

const getContactFrequencyText = (
  contactFrequency?: SupervisionContactFrequency
) => {
  if (!contactFrequency) {
    return null;
  }

  const [contacts, days] = contactFrequency;
  const pluralized = contacts === 1 ? "contact" : "contacts";
  return `${contacts} ${pluralized} every ${days} days`;
};

const NeedsFaceToFaceContact: React.FC<NeedsFaceToFaceContactProps> = ({
  className,
  client,
  onStatusChanged,
}: NeedsFaceToFaceContactProps) => {
  const [needChecked, setNeedChecked] = React.useState(false);
  React.useEffect(() => {
    setNeedChecked(
      client.caseUpdates[CaseUpdateActionType.SCHEDULED_FACE_TO_FACE]
        ?.status === CaseUpdateStatus.IN_PROGRESS
    );
  }, [client]);

  const { policyStore } = useRootStore();
  const {
    needsMet: { faceToFaceContact: met },
  } = client;

  const title = met
    ? `Face-to-Face Contact: Up To Date`
    : `Face-to-Face Contact Needed`;
  const contactFrequency = policyStore.findContactFrequencyForClient(client);

  const onToggleCheck = (checked: boolean) => {
    setNeedChecked(checked);
    onStatusChanged(checked);
  };

  return (
    <CaseCardBody className={className}>
      <Need
        kind={IconSVG.NeedsContact}
        state={
          client.needsMet.faceToFaceContact ? NeedState.MET : NeedState.NOT_MET
        }
      />
      <CaseCardInfo>
        <strong>{title}</strong>
        <br />
        <Caption>
          <div>
            <Icon kind={IconSVG.Place} size={10} fill={palette.text.caption} />{" "}
            {client.currentAddress || "No address on file"}
          </div>
          <div>{getContactFrequencyText(contactFrequency)}</div>
          {getLastContactedText(client)}
        </Caption>
        {!client.needsMet.faceToFaceContact ? (
          <ButtonContainer>
            <NeedsCheckboxButton
              checked={needChecked}
              onToggleCheck={onToggleCheck}
              title="I scheduled our next face-to-face contact"
            />
          </ButtonContainer>
        ) : null}
      </CaseCardInfo>
    </CaseCardBody>
  );
};

export default NeedsFaceToFaceContact;
