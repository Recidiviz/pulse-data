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
import { IconSVG, Need } from "@recidiviz/case-triage-components";
import * as React from "react";
import { CaseCardBody, CaseCardInfo, Caption } from "./CaseCard.styles";
import { DecoratedClient } from "../../stores/ClientsStore/Client";

interface NeedsEmploymentProps {
  className: string;
  client: DecoratedClient;
}

const NeedsEmployment: React.FC<NeedsEmploymentProps> = ({
  className,
  client,
}: NeedsEmploymentProps) => {
  const {
    needsMet: { employment: met },
  } = client;

  const title = met ? `Employer: ${client.employer}` : "Employment Needed";
  let caption = <Caption>Assumed employed from CIS</Caption>;
  if (!met) {
    const suffix = client.employer ? `: "${client.employer}"` : null;
    caption = <Caption>Assumed unemployed from CIS {suffix}</Caption>;
  }

  return (
    <CaseCardBody className={className}>
      <Need kind={IconSVG.NeedsEmployment} met={client.needsMet.employment} />
      <CaseCardInfo>
        <strong>{title}</strong>
        <br />
        {caption}
      </CaseCardInfo>
    </CaseCardBody>
  );
};

export default NeedsEmployment;
