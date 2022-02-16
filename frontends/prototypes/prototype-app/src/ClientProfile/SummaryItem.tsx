// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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

import { Icon, IconSVG, palette, spacing } from "@recidiviz/design-system";
import { rem } from "polished";
import React from "react";
import styled from "styled-components/macro";

const DetailsLineItem = styled.div`
  margin-bottom: ${rem(spacing.xs)};
`;

const SummaryLineItem = styled(DetailsLineItem)`
  display: grid;
  grid-template-columns: ${rem(24)} 1fr;
`;

const SummaryIcon = styled(Icon)`
  grid-column: 1;
  /* slight vertical offset to approximate baseline alignment */
  margin-top: ${rem(2)};
`;

const Summary = styled.div`
  grid-column: 2;
`;

const SummaryItem: React.FC = ({ children }) => {
  return (
    <SummaryLineItem>
      <SummaryIcon
        kind={IconSVG.Success}
        color={palette.signal.highlight}
        size={15}
      />
      <Summary>{children}</Summary>
    </SummaryLineItem>
  );
};

export default SummaryItem;
