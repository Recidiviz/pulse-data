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
import { Caption, CaseCardBody, CaseCardInfo } from "./CaseCard.styles";
import { DecoratedClient } from "../../stores/ClientsStore/Client";
import { useRootStore } from "../../stores";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { SupervisionContactFrequency } from "../../stores/PolicyStore/Policy";
import { NeedsActionFlow } from "./NeedsCorrectionDropdown";

interface NeedsFaceToFaceContactProps {
  className: string;
  client: DecoratedClient;
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

const getFrequencyText = (
  contactFrequency: SupervisionContactFrequency | undefined,
  singularUnit: string
) => {
  if (!contactFrequency) {
    return null;
  }

  const [contacts, days] = contactFrequency;
  const pluralized = contacts === 1 ? "" : "s";
  const daysPluralized = days === 1 ? "day" : "days";
  return `Policy: ${contacts} ${singularUnit}${pluralized} every ${days} ${daysPluralized}`;
};

const NeedsFaceToFaceContact: React.FC<NeedsFaceToFaceContactProps> = ({
  className,
  client,
}: NeedsFaceToFaceContactProps) => {
  const { policyStore } = useRootStore();
  const {
    needsMet: { faceToFaceContact: faceToFaceMet },
  } = client;

  const title = faceToFaceMet
    ? `Face to Face Contact: Up To Date`
    : `Face to Face Contact Needed`;
  const contactFrequency = policyStore.findContactFrequencyForClient(client);

  return (
    <CaseCardBody className={className}>
      <Need
        kind={IconSVG.NeedsContact}
        state={faceToFaceMet ? NeedState.MET : NeedState.NOT_MET}
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
            {getFrequencyText(contactFrequency, "contact")}
            <br />
            {getLastContactedText(client)}
          </div>
        </Caption>

        <NeedsActionFlow
          client={client}
          met={faceToFaceMet}
          resolve={CaseUpdateActionType.SCHEDULED_FACE_TO_FACE}
          dismiss={CaseUpdateActionType.INCORRECT_CONTACT_DATA}
        />
      </CaseCardInfo>
    </CaseCardBody>
  );
};

export default NeedsFaceToFaceContact;
