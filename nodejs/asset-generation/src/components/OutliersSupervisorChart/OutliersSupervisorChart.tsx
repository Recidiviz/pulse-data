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

import { spacing } from "@recidiviz/design-system";
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
  CONTENT_AREA_BOTTOM_OFFSET,
  CONTENT_AREA_TOP_OFFSET,
  HIGHLIGHT_DOT_RADIUS,
  LABEL_X_BASE,
  MARGIN,
  ROW_HEIGHT,
  SWARM_DOT_RADIUS,
  TICK_WIDTH,
} from "./constants";
import {
  AxisLabel,
  AxisSpine,
  RateHighlightChangeBar,
  RateHighlightMark,
  SwarmLabel,
  TargetLine,
} from "./styles";

const formatExtent = format(".0%");
const formatTarget = format(".1%");
const PREFERRED_SWARM_ASPECT_RATIO = 0.5;

function getValueExtent(data: ChartProps["data"]) {
  const allValues = [
    ...data.otherOfficers.map((o) => o.value),
    ...data.highlightedOfficers.map((o) => o.rate),
    ...data.highlightedOfficers
      .map((o) => o.prevRate)
      .filter((r): r is number => r !== null),
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
  prevX: number | null,
  width: number
) {
  // the previous value can be missing, which means it should have no effect on label position.
  // we can accomplish this by just setting it equal to cx, a known condition that has the exact same result
  const prevXNormalized = prevX ?? cx;

  let textAnchor: TextProps["textAnchor"] = "start";
  let x = LABEL_X_BASE;
  if (prevXNormalized > cx) {
    x += prevXNormalized - cx;
  }

  const rightOverflow = detectOverflow(labelText, 18, cx + x, width);

  if (rightOverflow) {
    x = LABEL_X_BASE * -1;
    textAnchor = "end";
    if (prevXNormalized < cx) {
      x -= cx - prevXNormalized;
    }
  }

  return { x, textAnchor };
}

type ChartProps = Pick<
  OutliersSupervisorChartInputTransformed,
  "data" | "width"
>;

export function OutliersSupervisorChart({ data, width }: ChartProps) {
  const { min, max } = getValueExtent(data);
  const xScale = scaleLinear()
    .domain([min, max])
    .range([MARGIN.left, width - MARGIN.right]);

  const highlightedRowsHeight = ROW_HEIGHT * data.highlightedOfficers.length;

  const preferredHeight =
    width * PREFERRED_SWARM_ASPECT_RATIO -
    (CONTENT_AREA_TOP_OFFSET + CONTENT_AREA_BOTTOM_OFFSET);

  const { swarmPoints, swarmSpread } = calculateSwarm({
    data: data.otherOfficers,
    radius: SWARM_DOT_RADIUS,
    valueScale: xScale,
    // make sure the swarm is large enough to contain the highlights
    minSpread: highlightedRowsHeight,
    // cap the chart height at a reasonable size, unless more space is needed to display highlights
    maxSpread:
      highlightedRowsHeight < preferredHeight ? preferredHeight : undefined,
  });

  const height = Math.ceil(
    CONTENT_AREA_TOP_OFFSET + CONTENT_AREA_BOTTOM_OFFSET + swarmSpread
  );

  // highlighted rows should be centered on the midpoint of the swarm
  const highlightedRowsOffset =
    CONTENT_AREA_TOP_OFFSET +
    // swarm will be >= highlights height because we passed minSpread above
    (swarmSpread - highlightedRowsHeight) / 2 +
    ROW_HEIGHT / 2;

  const axisPositions = {
    tickStart: MARGIN.top,
    tickEnd: height - CONTENT_AREA_BOTTOM_OFFSET,
    spine: height - CONTENT_AREA_BOTTOM_OFFSET,
    axisLabel: height - MARGIN.bottom,
    min: xScale(min),
    max: xScale(max),
    target: xScale(data.target),
  };

  return (
    <svg width={width} height={height}>
      {/* axis */}
      <AxisSpine
        x1={axisPositions.min}
        x2={axisPositions.max}
        y1={axisPositions.spine}
        y2={axisPositions.spine}
      />
      <AxisLabel
        x={axisPositions.min}
        y={axisPositions.axisLabel}
        verticalAnchor="end"
      >
        {formatExtent(min)}
      </AxisLabel>
      <AxisLabel
        textAnchor="end"
        x={axisPositions.max}
        y={axisPositions.axisLabel}
        verticalAnchor="end"
      >
        {formatExtent(max)}
      </AxisLabel>

      {/* target */}
      <TargetLine
        x1={axisPositions.target}
        x2={axisPositions.target}
        y1={axisPositions.tickStart}
        y2={axisPositions.tickEnd}
      />
      <AxisLabel
        dx={TICK_WIDTH + spacing.xxs}
        x={axisPositions.target}
        y={axisPositions.tickStart}
        verticalAnchor="start"
      >
        {formatTarget(data.target)}
      </AxisLabel>

      {/* chart content */}
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
            const prevX = e.prevRate !== null ? xScale(e.prevRate) : null;
            const labelText = e.name;

            return (
              <g
                key={labelText}
                transform={`translate(${cx} ${ROW_HEIGHT * i})`}
              >
                {prevX !== null && (
                  <RateHighlightChangeBar
                    // this rect spans the distance from the outer edges of two circles
                    // centered at cx & prevX
                    width={HIGHLIGHT_DOT_RADIUS * 2 + Math.abs(cx - prevX)}
                    rx={HIGHLIGHT_DOT_RADIUS}
                    y={-HIGHLIGHT_DOT_RADIUS}
                    // the left edge position depends on which way the bar extends from cx
                    x={(cx < prevX ? 0 : prevX - cx) - HIGHLIGHT_DOT_RADIUS}
                  />
                )}
                <RateHighlightMark
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
