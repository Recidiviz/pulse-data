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
import { Icon, IconSVG, Need, NeedState } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import * as React from "react";
import { useRootStore } from "../../stores";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";
import { Client } from "../../stores/ClientsStore";
import { NeedsActionFlow } from "../NeedsActionFlow/NeedsActionFlow";
import TEST_IDS from "../TestIDs";
import { Caption, CaseCardBody, CaseCardInfo } from "./CaseCard.styles";
import { getContactFrequencyText, getLastContactedText } from "./strings";

interface NeedsFaceToFaceContactProps {
  className: string;
  client: Client;
}

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
  const contactFrequencyText = getContactFrequencyText(contactFrequency);

  return (
    <CaseCardBody
      className={className}
      data-testid={TEST_IDS.NEEDS_FACE_TO_FACE_CONTACT}
    >
      <Need
        kind={IconSVG.NeedsContact}
        state={faceToFaceMet ? NeedState.MET : NeedState.NOT_MET}
      />
      <CaseCardInfo>
        <strong>{title}</strong>
        <br />
        <Caption>
          <div>
            <Icon kind={IconSVG.Place} size={10} />{" "}
            {client.currentAddress || "No address on file"}
          </div>
          <div>
            {contactFrequencyText && (
              <>
                {contactFrequencyText}
                <br />
              </>
            )}
            {getLastContactedText(client)}
          </div>
        </Caption>

        <NeedsActionFlow
          actionable={!faceToFaceMet}
          client={client}
          resolve={CaseUpdateActionType.SCHEDULED_FACE_TO_FACE}
          dismiss={CaseUpdateActionType.INCORRECT_CONTACT_DATA}
        />
      </CaseCardInfo>
    </CaseCardBody>
  );
};

export default observer(NeedsFaceToFaceContact);
