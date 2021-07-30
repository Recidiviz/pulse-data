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

import { Link } from "@recidiviz/design-system";
import React from "react";
import { useRootStore } from "../../stores";
import { Opportunity } from "../../stores/OpportunityStore";

type PolicyLinkProps = {
  opportunity: Opportunity;
};

export const PolicyLink = ({
  opportunity,
}: PolicyLinkProps): JSX.Element | null => {
  const { policyStore } = useRootStore();

  const policyText = `${policyStore.getDOCName()} policy`;
  const policyLink =
    policyStore.policies?.policyReferencesForOpportunities[
      opportunity.opportunityType
    ];
  const policyLinkElement = policyLink ? (
    <Link href={policyLink} target="_blank" rel="noopener noreferrer">
      {policyText}
    </Link>
  ) : (
    <span>{policyText}</span>
  );

  return policyLinkElement;
};
