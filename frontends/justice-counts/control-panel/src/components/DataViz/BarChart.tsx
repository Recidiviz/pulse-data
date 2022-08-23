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
import {
  Bar,
  BarChart as BarChartComponent,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip as TooltipComponent,
  XAxis,
  YAxis,
} from "recharts";
import styled from "styled-components/macro";

import { Datapoint } from "../../shared/types";
import { rem } from "../../utils";
import { palette } from "../GlobalStyles";
import Tooltip from "./Tooltip";
import {
  getDatapointDimensions,
  sortDatapointDimensions,
  splitUtcString,
} from "./utils";

const MAX_BAR_SIZE = 150;

const ChartContainer = styled.div`
  display: flex;
  flex-grow: 1;
  justify-content: center;
  align-items: center;
`;

const NoReportedData = styled.div`
  position: absolute;
  background: white;
  padding: 8px;

  &::after {
    content: "No reported data for this metric.";
  }
`;

const tickStyle = {
  fontSize: rem("12px"),
  fontWeight: 600,
  fill: palette.solid.darkgrey,
};

const abbreviateNumber = (num: number) => {
  // abbreviates numbers into 1k, 2.5m, 5.5t, etc
  const numLength = num.toString().length;
  if (numLength >= 13) {
    return `${parseFloat((num / 1000000000000).toFixed(1))}t`;
  }
  if (numLength >= 10) {
    return `${parseFloat((num / 1000000000).toFixed(1))}b`;
  }
  if (numLength >= 7) {
    return `${parseFloat((num / 1000000).toFixed(1))}m`;
  }
  if (numLength >= 4) {
    return `${parseFloat((num / 1000).toFixed(1))}k`;
  }
  return num.toString();
};

interface TickProps {
  y: number;
  payload: {
    coordinate: number;
    isShow: boolean;
    offset: number;
    tickCoord: number;
    value: number;
  };
}

interface CustomYAxisTickProps extends TickProps {
  percentageView: boolean;
}

const CustomYAxisTick = (props: CustomYAxisTickProps) => {
  const { y, payload, percentageView } = props;
  const str = percentageView
    ? `${payload.value * 100}%`
    : abbreviateNumber(payload.value);
  const label = str.length > 7 ? str.substring(0, 5).concat("...") : str;
  return (
    <g transform={`translate(${0},${y})`}>
      <text
        x={0}
        y={0}
        textAnchor="start"
        fill={palette.solid.darkgrey}
        style={tickStyle}
      >
        {label}
      </text>
    </g>
  );
};

const CustomCursor = (props: React.SVGProps<SVGRectElement>) => {
  const { x, y, width, height } = props;
  return (
    <rect
      fill={palette.solid.darkgrey}
      x={Number(x) + Number(width) / 2}
      y={y}
      width={1}
      height={height}
    />
  );
};

const ResponsiveBarChart: React.FC<{
  data: Datapoint[];
  percentageView?: boolean;
}> = ({ data, percentageView = false }) => {
  const isAnnual = data[0]?.frequency === "ANNUAL";
  const renderBarDefinitions = () => {
    // each Recharts Bar component defines a category type in the stacked bar chart
    const barDefinitions = [];
    const dimensions = getDatapointDimensions(data[0]);
    Object.keys(dimensions)
      .sort(sortDatapointDimensions)
      .forEach((key, index) => {
        barDefinitions.push(
          <Bar
            key={key}
            dataKey={key}
            stackId="a"
            fill={Object.values(palette.dataViz)[index]}
            maxBarSize={MAX_BAR_SIZE}
          />
        );
      });
    barDefinitions.push(
      <Bar
        key="dataVizMissingData"
        dataKey="dataVizMissingData"
        stackId="a"
        fill="url(#gradient)"
        maxBarSize={MAX_BAR_SIZE}
      />
    );
    return barDefinitions;
  };

  return (
    <ChartContainer>
      <ResponsiveContainer width="100%" height="100%">
        <BarChartComponent
          data={data}
          barGap={0}
          barCategoryGap={0.5}
          margin={{
            top: 20,
            right: 0,
            left: 0,
            bottom: 16,
          }}
        >
          <defs>
            <linearGradient
              key="gradient"
              x1="0"
              x2="0"
              y1="0"
              y2="1"
              id="gradient"
            >
              <stop stopColor="rgba(221, 18, 18, 0)" offset="0%" />
              <stop stopColor="rgba(221, 18, 18, 0.25)" offset="100%" />
            </linearGradient>
          </defs>
          <CartesianGrid vertical={false} strokeDasharray="1 0" />
          <XAxis
            dataKey="start_date"
            padding={{ left: -0.5, right: -0.5 }}
            interval="preserveEnd"
            minTickGap={32}
            tick={tickStyle}
            tickLine={false}
            tickFormatter={(value) => {
              if (data.length === 0) {
                return "";
              }
              const [, , month, year] = splitUtcString(value);
              return isAnnual ? year : `${month} ${year}`;
            }}
            tickMargin={12}
          />
          <YAxis
            allowDecimals={percentageView}
            tick={(props: TickProps) => (
              <CustomYAxisTick
                y={props.y}
                payload={props.payload}
                percentageView={percentageView}
              />
            )}
            tickLine={false}
            tickCount={percentageView ? 5 : 12}
            domain={percentageView ? ["dataMin", "dataMax"] : undefined}
            axisLine={false}
          />
          <TooltipComponent
            isAnimationActive={false}
            position={{ y: 100 }}
            cursor={<CustomCursor />}
            content={
              <Tooltip percentOnly={percentageView} isAnnual={isAnnual} />
            }
          />
          {renderBarDefinitions()}
        </BarChartComponent>
      </ResponsiveContainer>
      {data.length === 0 && <NoReportedData />}
    </ChartContainer>
  );
};

export default ResponsiveBarChart;
