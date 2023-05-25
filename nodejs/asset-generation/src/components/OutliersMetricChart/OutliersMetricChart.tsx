// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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

import { palette, spacing, typography } from "@recidiviz/design-system";
import { Text } from "@visx/text";
import { descending } from "d3-array";
import { format } from "d3-format";
import { scaleLinear, scalePoint } from "d3-scale";
import { styled } from "styled-components";

import { ChartData } from "../../server/generate/outliersMetricChart/types";
import { renderToStaticSvg } from "../utils";
import {
  AXIS_OFFSET,
  GOAL_COLORS,
  MARGIN,
  ROW_HEIGHT,
  ROWS_OFFSET,
  TICK_WIDTH,
} from "./constants";
import { RateMark } from "./RateMark";
import { RowLabel } from "./RowLabel";

const formatExtent = format(".0%");
const formatGoal = format(".1%");

const AxisLabel = styled(Text).attrs({ verticalAnchor: "start" })`
  ${typography.Sans16}
  fill: ${palette.slate60};
`;

const TickLine = styled.line`
  stroke: ${palette.slate20};
  stroke-width: ${TICK_WIDTH}px;
`;

const GoalLine = styled.line`
  stroke: ${palette.slate60};
  stroke-width: ${TICK_WIDTH}px;
  stroke-dasharray: 4 4;
`;

type ChartProps = {
  data: ChartData;
  width: number;
  entityLabel: string;
};

/**
 * Computes desired chart height based on the number of records in the input data.
 */
export function calculateChartHeight(data: ChartData) {
  return ROWS_OFFSET + ROW_HEIGHT * data.entities.length;
}

export function OutliersMetricChart({ data, width, entityLabel }: ChartProps) {
  const { min, max, goal, entities } = data;

  const rows = entities.sort((a, b) => descending(a.rate, b.rate));

  const chartDomain = [min, max];
  // round domain off to full percentages
  chartDomain[0] = Math.floor(chartDomain[0] * 100) / 100;
  chartDomain[1] = Math.ceil(chartDomain[1] * 100) / 100;

  const xScale = scaleLinear()
    .domain(chartDomain)
    .range([AXIS_OFFSET, width - MARGIN.right]);

  const height = calculateChartHeight(data);
  const yScale = scalePoint<number>()
    .domain(rows.map((r, i) => i))
    .range([ROWS_OFFSET, height])
    .padding(0.5);

  const axisPositions = {
    tickStart: MARGIN.top,
    tickEnd: height - MARGIN.bottom,
    min: xScale(chartDomain[0]),
    max: xScale(chartDomain[1]),
    goal: xScale(goal),
  };

  return (
    <svg style={{ width, height }}>
      {/* entity label */}
      <AxisLabel verticalAnchor="start" x={MARGIN.left} y={MARGIN.top}>
        {entityLabel}
      </AxisLabel>

      {/* axis min */}
      <TickLine
        x1={axisPositions.min}
        x2={axisPositions.min}
        y1={axisPositions.tickStart}
        y2={axisPositions.tickEnd}
      />
      <AxisLabel
        dx={TICK_WIDTH + spacing.xxs}
        x={axisPositions.min}
        y={axisPositions.tickStart}
      >
        {formatExtent(chartDomain[0])}
      </AxisLabel>

      {/* axis max */}
      <TickLine
        x1={axisPositions.max}
        x2={axisPositions.max}
        y1={axisPositions.tickStart}
        y2={axisPositions.tickEnd}
      />
      <AxisLabel
        dx={-(TICK_WIDTH + spacing.xxs)}
        textAnchor="end"
        x={axisPositions.max}
        y={axisPositions.tickStart}
      >
        {formatExtent(chartDomain[1])}
      </AxisLabel>

      {/* axis goal/avg */}
      <GoalLine
        x1={axisPositions.goal}
        x2={axisPositions.goal}
        y1={axisPositions.tickStart}
        y2={axisPositions.tickEnd}
      />
      <AxisLabel
        dx={TICK_WIDTH + spacing.xxs}
        x={axisPositions.goal}
        y={axisPositions.tickStart}
      >
        {formatGoal(goal)}
      </AxisLabel>

      {rows.map((r, i) => {
        return (
          // eslint-disable-next-line react/no-array-index-key
          <g key={i}>
            <RowLabel x={MARGIN.left} y={yScale(i)}>
              {r.name}
            </RowLabel>
            <RateMark
              x={xScale(r.rate)}
              y={yScale(i)}
              filled
              color={GOAL_COLORS[r.goalStatus]}
            />
            <RateMark
              x={xScale(r.previousRate)}
              y={yScale(i)}
              color={GOAL_COLORS[r.previousGoalStatus]}
            />
          </g>
        );
      })}
    </svg>
  );
}

export function getRenderedChartSvg(props: ChartProps) {
  const height = calculateChartHeight(props.data);
  return {
    height,
    svg: renderToStaticSvg(() => <OutliersMetricChart {...props} />),
  };
}
