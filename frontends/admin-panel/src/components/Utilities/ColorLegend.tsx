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

import { palette } from "@recidiviz/design-system";
import { rem } from "polished";
import React from "react";
import styled from "styled-components/macro";

const ColorLegendWrapper = styled.div`
  display: flex;
  flex-wrap: wrap;
  font-size: ${rem(12)};
`;

const animationDuration = 500;

const ColorLegendItem = styled.div`
  align-items: center;
  color: ${palette.data.forest2};
  cursor: pointer;
  display: flex;
  flex: 0 0 auto;
  margin-right: 8px;
  padding-bottom: 4px;
  position: relative;
  white-space: nowrap;

  &:last-child {
    margin-right: 0;
  }

  &::after {
    background: ${(props) => props.color};
    bottom: 0;
    content: "";
    display: block;
    height: 1px;
    left: 50%;
    position: absolute;
    transition: width ${animationDuration}ms, left ${animationDuration}ms;
    width: 0;
  }

  &.highlighted::after {
    width: 100%;
    left: 0;
  }
`;

const ColorLegendItemLabel = styled.div``;
const swatchSize = 8;
const ColorLegendItemSwatch = styled.div`
  background: ${(props) => props.color};
  border-radius: ${swatchSize / 2}px;
  height: ${swatchSize}px;
  margin-left: ${swatchSize / 2}px;
  transition: background-color ${animationDuration}ms;
  width: ${swatchSize}px;
`;

const ColorLegend: React.FC<{
  items: { title: string; color: string }[];
}> = ({ items }) => {
  return (
    <ColorLegendWrapper aria-hidden>
      {items.map(({ title, color }) => (
        <ColorLegendItem color={color} key={title}>
          <ColorLegendItemLabel>{title}</ColorLegendItemLabel>
          <ColorLegendItemSwatch color={color} />
        </ColorLegendItem>
      ))}
    </ColorLegendWrapper>
  );
};

export default ColorLegend;
