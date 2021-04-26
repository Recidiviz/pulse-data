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
import {
  CaseUpdateActionType,
  CaseUpdateStatus,
} from "../../stores/CaseUpdatesStore";
import { SupervisionContactFrequency } from "../../stores/PolicyStore/Policy";

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

const getLastHomeVisitText = (client: DecoratedClient) => {
  const { mostRecentHomeVisitDate } = client;

  if (mostRecentHomeVisitDate) {
    return `Last home visit on ${mostRecentHomeVisitDate.format(
      "MMMM Do, YYYY"
    )}`;
  }
  return `Assumed no home visit from CIS.`;
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

const getHomeVisitFrequencyText = (
  homeVisitFrequency?: SupervisionContactFrequency
) => {
  if (!homeVisitFrequency) {
    return null;
  }

  const [homeVisit, days] = homeVisitFrequency;
  const homeVisitPluralized = homeVisit === 1 ? "home visit" : "home visits";
  const daysPluralized = days === 1 ? "day" : "days";
  return `${homeVisit} ${homeVisitPluralized} every ${days} ${daysPluralized}`;
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
    needsMet: {
      faceToFaceContact: faceToFaceMet,
      homeVisitContact: homeVisitMet,
    },
  } = client;

  const title =
    faceToFaceMet && homeVisitMet
      ? `Face-to-Face Contact: Up To Date`
      : `Face-to-Face Contact Needed`;
  const contactFrequency = policyStore.findContactFrequencyForClient(client);
  const homeVisitFrequency = policyStore.findHomeVisitFrequencyForClient(
    client
  );

  const onToggleCheck = (checked: boolean) => {
    setNeedChecked(checked);
    onStatusChanged(checked);
  };

  return (
    <CaseCardBody className={className}>
      <Need
        kind={IconSVG.NeedsContact}
        state={
          client.needsMet.faceToFaceContact && client.needsMet.homeVisitContact
            ? NeedState.MET
            : NeedState.NOT_MET
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
          <div>
            {getContactFrequencyText(contactFrequency)}.{" "}
            {getLastContactedText(client)}
          </div>
          <div>
            {getHomeVisitFrequencyText(homeVisitFrequency)}.{" "}
            {getLastHomeVisitText(client)}
          </div>
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
