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

import React from "react";
import styled from "styled-components/macro";

import { palette } from "../GlobalStyles";

const EmptyLegendContainer = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
  flex-wrap: wrap;
  margin-bottom: 20px;
  height: 50px;
`;

const LegendContainer = styled(EmptyLegendContainer)`
  border-top: 1px solid rgba(23, 28, 43, 0.6);
  padding: 16px 0;
  border-bottom: 1px solid rgba(23, 28, 43, 0.6);
`;

const LegendItem = styled.div`
  display: flex;
  margin-right: 8px;
  flex-direction: row;
  align-items: center;
`;

export const LegendColor = styled.div<{
  index: number;
}>`
  width: 8px;
  height: 8px;
  margin-right: 4px;
  background: ${({ index }) => Object.values(palette.dataViz)[index]};
  border-radius: 10px;
`;

const LegendText = styled.div`
  font-size: 12px;
`;

const Legend: React.FC<{
  names?: string[];
}> = ({ names }) => {
  if (names) {
    return (
      <LegendContainer>
        {names.map((name, idx) => (
          <LegendItem key={name}>
            <LegendColor index={idx} />
            <LegendText>{name}</LegendText>
          </LegendItem>
        ))}
      </LegendContainer>
    );
  }

  return <EmptyLegendContainer />;
};

export default Legend;
