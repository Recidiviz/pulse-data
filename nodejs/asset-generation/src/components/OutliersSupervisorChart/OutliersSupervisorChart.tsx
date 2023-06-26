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

import { palette, spacing } from "@recidiviz/design-system";
import { TextProps } from "@visx/text";
import { descending } from "d3-array";
import { format } from "d3-format";
import { scaleLinear } from "d3-scale";

import { OutliersSupervisorChartInputTransformed } from "../../server/generate/outliersSupervisorChart/types";
import { OUTLIERS_GOAL_COLORS } from "../constants";
import { SwarmedCircleGroup } from "../SwarmedCircleGroup";
import { calculateSwarm } from "../SwarmedCircleGroup/calculateSwarm";
import { computeTextWidth } from "../utils";
import {
  CONTENT_AREA_TOP_OFFSET,
  HIGHLIGHT_DOT_RADIUS,
  LABEL_X_BASE,
  MARGIN,
  ROW_HEIGHT,
  SWARM_DOT_RADIUS,
  TICK_WIDTH,
  X_AXIS_HEIGHT,
} from "./constants";
import { AxisLabel, GoalLine, SwarmLabel, TickLine } from "./styles";

const formatExtent = format(".0%");
const formatTarget = format(".1%");

function getValueExtent(data: ChartProps["data"]) {
  const allValues = [
    ...data.otherOfficers.map((o) => o.value),
    ...data.highlightedOfficers.map((o) => [o.rate, o.prevRate]),
  ]
    .flat()
    .sort();

  // round extrema to the nearest whole percentage point
  const min = Math.floor(allValues[0] * 100) / 100;
  const max = Math.ceil(allValues.slice(-1)[0] * 100) / 100;
  return { min, max };
}

function detectOverflow(
  text: string,
  fontSizePx: number,
  startPosition: number,
  limit: number
): boolean {
  const { totalWidth } = computeTextWidth(text, fontSizePx);
  return totalWidth > limit - startPosition;
}

/**
 * The label sits at the outer edge of the change-over-time bar, and it flips
 * to the left side if it doesn't fit on the right. This function figures all that
 * out and returns the appropriate `text` props.
 */
function getLabelProps(
  labelText: string,
  cx: number,
  prevX: number,
  width: number
) {
  let textAnchor: TextProps["textAnchor"] = "start";
  let x = LABEL_X_BASE;
  if (prevX > cx) {
    x += prevX - cx;
  }

  const rightOverflow = detectOverflow(labelText, 18, cx + x, width);

  if (rightOverflow) {
    x = LABEL_X_BASE * -1;
    textAnchor = "end";
    if (prevX < cx) {
      x -= cx - prevX;
    }
  }

  return { x, textAnchor };
}

type ChartProps = Pick<
  OutliersSupervisorChartInputTransformed,
  "data" | "width"
> & {
  // this lets the component share its dynamically calculated height with its parent
  syncHeight: (h: number) => void;
};

export function OutliersSupervisorChart({
  data,
  width,
  syncHeight,
}: ChartProps) {
  const { min, max } = getValueExtent(data);
  const xScale = scaleLinear()
    .domain([min, max])
    .range([MARGIN.left, width - MARGIN.right]);

  const highlightedRowsHeight = ROW_HEIGHT * data.highlightedOfficers.length;

  const { swarmPoints, swarmSpread } = calculateSwarm({
    data: data.otherOfficers,
    radius: SWARM_DOT_RADIUS,
    valueScale: xScale,
    // make sure the swarm is large enough to contain the highlights
    minSpread: highlightedRowsHeight,
  });

  const height = Math.ceil(
    MARGIN.top + MARGIN.bottom + X_AXIS_HEIGHT + swarmSpread
  );

  // because of server rendering, there's no guarantee that effect hooks will run,
  // so we can just call this during render. it should not trigger a re-render
  syncHeight(height);

  // highlighted rows should be centered on the midpoint of the swarm
  const highlightedRowsOffset =
    CONTENT_AREA_TOP_OFFSET +
    // swarm will be >= highlights height because we passed minSpread above
    (swarmSpread - highlightedRowsHeight) / 2 +
    ROW_HEIGHT / 2;

  const axisPositions = {
    tickStart: MARGIN.top,
    tickEnd: height - MARGIN.bottom,
    min: xScale(min),
    max: xScale(max),
    target: xScale(data.target),
  };

  return (
    <svg width={width} height={height}>
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
        {formatExtent(min)}
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
        {formatExtent(max)}
      </AxisLabel>

      {/* axis target */}
      <GoalLine
        x1={axisPositions.target}
        x2={axisPositions.target}
        y1={axisPositions.tickStart}
        y2={axisPositions.tickEnd}
      />
      <AxisLabel
        dx={TICK_WIDTH + spacing.xxs}
        x={axisPositions.target}
        y={axisPositions.tickStart}
      >
        {formatTarget(data.target)}
      </AxisLabel>

      <SwarmedCircleGroup
        data={swarmPoints}
        radius={SWARM_DOT_RADIUS}
        opacity={0.25}
        transform={`translate(0 ${CONTENT_AREA_TOP_OFFSET + swarmSpread / 2})`}
      />

      {/* highlighted officers */}
      <g transform={`translate(0 ${highlightedRowsOffset})`}>
        {data.highlightedOfficers
          .sort((a, b) => descending(a.rate, b.rate))
          .map((e, i) => {
            const cx = xScale(e.rate);
            const prevX = xScale(e.prevRate);
            const labelText = e.name;

            return (
              <g
                key={labelText}
                transform={`translate(${cx} ${ROW_HEIGHT * i})`}
              >
                <rect
                  height={HIGHLIGHT_DOT_RADIUS * 2}
                  // this rect spans the distance from the outer edges of two circles
                  // centered at cx & prevX
                  width={HIGHLIGHT_DOT_RADIUS * 2 + Math.abs(cx - prevX)}
                  fill={palette.slate20}
                  rx={HIGHLIGHT_DOT_RADIUS}
                  y={-HIGHLIGHT_DOT_RADIUS}
                  // the left edge position depends on which way the bar extends from cx
                  x={(cx < prevX ? 0 : prevX - cx) - HIGHLIGHT_DOT_RADIUS}
                />
                <circle
                  r={HIGHLIGHT_DOT_RADIUS}
                  fill={OUTLIERS_GOAL_COLORS[e.targetStatus]}
                />
                <SwarmLabel {...getLabelProps(labelText, cx, prevX, width)}>
                  {labelText}
                </SwarmLabel>
              </g>
            );
          })}
      </g>
    </svg>
  );
}
