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
import { IconSVG, Need, NeedState } from "@recidiviz/design-system";
import * as React from "react";
import {
  Caption,
  CaseCardBody,
  CaseCardInfo,
  CheckboxButtonContainer,
} from "./CaseCard.styles";
import { NeedsCheckboxButton } from "./NeedsCheckboxButton";
import { DecoratedClient } from "../../stores/ClientsStore/Client";
import { CaseUpdateActionType } from "../../stores/CaseUpdatesStore";

interface NeedsEmploymentProps {
  className: string;
  client: DecoratedClient;

  onStatusChanged: (helped: boolean) => void;
}

const NeedsEmployment: React.FC<NeedsEmploymentProps> = ({
  className,
  client,
  onStatusChanged,
}: NeedsEmploymentProps) => {
  const [needChecked, setNeedChecked] = React.useState(false);
  React.useEffect(() => {
    setNeedChecked(false);
  }, [client]);

  const {
    needsMet: { employment: met },
  } = client;

  const title = met ? `Employer: ${client.employer}` : "Employment Needed";
  let caption = <Caption>Assumed employed from CIS</Caption>;
  if (!met) {
    const suffix = client.employer ? `: "${client.employer}"` : null;
    caption = <Caption>Assumed unemployed from CIS {suffix}</Caption>;
  }

  const onToggleCheck = (checked: boolean) => {
    setNeedChecked(checked);
    onStatusChanged(checked);
  };

  return (
    <CaseCardBody className={className}>
      <Need
        kind={IconSVG.NeedsEmployment}
        state={client.needsMet.employment ? NeedState.MET : NeedState.NOT_MET}
      />
      <CaseCardInfo>
        <strong>{title}</strong>
        <br />
        {caption}
        {!client.needsMet.employment ? (
          <CheckboxButtonContainer>
            <NeedsCheckboxButton
              checked={needChecked}
              inProgress={client.inProgressActions?.includes(
                CaseUpdateActionType.FOUND_EMPLOYMENT
              )}
              onToggleCheck={onToggleCheck}
            >
              I helped them find employment
            </NeedsCheckboxButton>
          </CheckboxButtonContainer>
        ) : null}
      </CaseCardInfo>
    </CaseCardBody>
  );
};

export default NeedsEmployment;
