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
import { TooltipProps as RechartsTooltipProps } from "recharts";
import styled from "styled-components/macro";

import { Datapoint } from "../../shared/types";
import { formatNumberInput } from "../../utils";
import { palette, typography } from "../GlobalStyles";
import { LegendColor } from "./Legend";
import {
  getDatapointDimensions,
  getSumOfDimensionValues,
  sortDatapointDimensions,
  splitUtcString,
} from "./utils";

const TooltipContainer = styled.div`
  padding: 16px;
  border-radius: 4px;
  background: ${palette.solid.darkgrey};
`;

const TooltipItemContainer = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-between;
`;

const TooltipName = styled.div`
  ${typography.sizeCSS.normal}
  flex-grow: 1;
  color: ${palette.solid.white};
`;

const TooltipValue = styled(TooltipName)`
  flex-grow: 0;
  margin-left: 32px;
`;

const TooltipNameWithBottomMargin = styled(TooltipName)`
  margin-bottom: 16px;
`;

interface TooltipProps extends RechartsTooltipProps<number, string> {
  percentOnly: boolean;
  isAnnual: boolean;
}

const Tooltip: React.FC<TooltipProps> = ({
  active,
  payload,
  label,
  percentOnly,
  isAnnual,
}) => {
  if (active && payload && payload.length) {
    const [, , month, year] = label ? splitUtcString(label) : [];

    const renderText = (val: string | number | null, maxValue: number) => {
      if (typeof val !== "number") {
        return "Not Reported";
      }

      let percentText = `${
        val !== 0 ? Math.round((val / maxValue) * 100) : 0
      }%`;
      // handle case of non-zero being rounded down to 0%
      if (percentText === "0%" && val !== 0) {
        percentText = "<1%";
      }
      return percentOnly
        ? percentText
        : `${formatNumberInput(val.toString())}${
            payload.length > 2 ? ` (${percentText})` : ""
          }`;
    };

    const renderItems = () => {
      if (payload.length === 0) {
        return null;
      }

      const datapoint = payload[0].payload as Datapoint;
      if (datapoint.dataVizMissingData !== 0) {
        return (
          <TooltipItemContainer>
            <TooltipName>Not reported</TooltipName>
          </TooltipItemContainer>
        );
      }

      const dimensions = getDatapointDimensions(datapoint);

      const sumOfDimensions = getSumOfDimensionValues(datapoint);

      return Object.keys(dimensions)
        .sort(sortDatapointDimensions)
        .map((key, idx: number) => {
          if (key === "dataVizMissingData") {
            return null;
          }

          return (
            <TooltipItemContainer key={key}>
              <LegendColor index={idx} />
              <TooltipName>{key}</TooltipName>
              <TooltipValue>
                {renderText(dimensions[key], sumOfDimensions)}
              </TooltipValue>
            </TooltipItemContainer>
          );
        });
    };

    return (
      <TooltipContainer>
        <TooltipNameWithBottomMargin>
          {isAnnual ? year : `${month} ${year}`}
        </TooltipNameWithBottomMargin>
        {renderItems()}
      </TooltipContainer>
    );
  }

  return null;
};

export default Tooltip;
