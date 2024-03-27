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

import { ChartWrapper, palette } from "@recidiviz/design-system";
import {
  Badge,
  Card,
  Descriptions,
  Divider,
  Row,
  Space,
  Spin,
  Timeline,
  Typography,
} from "antd";
import { scaleTime } from "d3-scale";
import * as React from "react";
import XYFrame from "semiotic/lib/XYFrame";

import {
  IngestInstanceStatusInfo,
  IngestStatus,
} from "../IngestStatus/constants";
import ColorLegend from "../Utilities/ColorLegend";
import { formatDatetime } from "../Utilities/GeneralUtilities";
import {
  IngestStatusRefreshInfo,
  ValidationDetailsGraphProps,
} from "./constants";
import { formatDate, formatStatusAmount } from "./utils";

const filterToRefreshesDuringWindow = (
  startTime: Date | undefined,
  endTime: Date,
  ingestEvents: IngestStatusRefreshInfo[]
): IngestStatusRefreshInfo[] => {
  return ingestEvents.filter(({ refreshTimestamp }) => {
    const refreshTime = new Date(refreshTimestamp);
    return (
      (startTime === undefined || startTime <= refreshTime) &&
      refreshTime < endTime
    );
  });
};

const getLabelForStatus = (status: IngestStatus) => {
  switch (status) {
    case IngestStatus.FLASH_COMPLETED:
      return "Rerun Flashed";
    case IngestStatus.RAW_DATA_IMPORT_IN_PROGRESS:
      return "Raw Data Import";
    case IngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS:
      return "Ingest View Materialization";
    case IngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS:
      return "Entity Ingest";
    case IngestStatus.UP_TO_DATE:
      return "Ingest Up to Date";
    case IngestStatus.RAW_DATA_UP_TO_DATE:
      return "Raw Data Import Up to Date";
    default:
      return "";
  }
};

const getLabelForStatuses = (statuses: IngestInstanceStatusInfo[]): string => {
  const statusNames = statuses.map((statusInfo) => statusInfo.status);
  // If there was a flash, ignore other statuses.
  if (statusNames.includes(IngestStatus.FLASH_COMPLETED)) {
    return getLabelForStatus(IngestStatus.FLASH_COMPLETED);
  }

  const labels = [];
  if (statusNames.includes(IngestStatus.RAW_DATA_IMPORT_IN_PROGRESS)) {
    labels.push(getLabelForStatus(IngestStatus.RAW_DATA_IMPORT_IN_PROGRESS));
  }
  if (statusNames.includes(IngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS)) {
    labels.push(getLabelForStatus(IngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS));
  }
  return labels.join(" & ");
};

const ValidationDetailsGraph: React.FC<ValidationDetailsGraphProps> = ({
  records,
  isPercent,
  loading,
  ingestEvents,
  versionChanges,
}) => {
  const lines = [
    {
      title: "Error",
      color: palette.data.forest1,
      coordinates: records.map((record, index) => ({
        time: record.getRunDatetime()?.toDate(),
        error: record.getErrorAmount(),
      })),
    },
    {
      title: "Soft Threshold",
      color: palette.data.gold1,
      coordinates: records.map((record, index) => ({
        time: record.getRunDatetime()?.toDate(),
        error: record.getSoftFailureAmount(),
      })),
    },
    {
      title: "Hard Threshold",
      color: palette.data.crimson1,
      coordinates: records.map((record, index) => ({
        time: record.getRunDatetime()?.toDate(),
        error: record.getHardFailureAmount(),
      })),
    },
  ];
  const yMax =
    1.1 *
    Math.max(
      ...records.reduce(
        (acc, record) =>
          acc.concat(
            record.getErrorAmount() || 0,
            record.getSoftFailureAmount() || 0,
            record.getHardFailureAmount() || 0
          ),
        [] as number[]
      )
    );
  const tooltipContent = (d: { time: Date; voronoiX: number; y: number }) => {
    // TODO(#20103): add more ingest information to tooltip (what actual data was
    // ingested?)
    const pointIdx = lines[0].coordinates.findIndex((point) => {
      return point.time?.getTime() === d.time?.getTime();
    });
    const points = lines.map((line) => {
      return {
        title: line.title,
        color: line.color,
        data: line.coordinates[pointIdx],
      };
    });
    // Shifts the tooltip to the left of the point if the point is on the right half of
    // the graph. This is comparing to the x coordinate of the point.
    const flipLeft = d.voronoiX > 350;
    const tx = flipLeft ? "-100%" : "0";
    // Shifts the tooltip above the point if the point is on the bottom 3/4 of the
    // graph. This is comparing to the y value for this point retrieved via `yAccessor`.
    const ty = d.y < 0.75 * yMax ? "-100%" : "0";

    // Flip if necessary, then add an x offset to make the vertical bar more visible.
    const transformStyle = `translate(${tx}, ${ty}) translateX(${
      flipLeft ? "-8px" : "8px"
    })`;

    const relevantIngestEvents = filterToRefreshesDuringWindow(
      pointIdx !== 0 ? lines[0].coordinates[pointIdx - 1].time : undefined,
      lines[0].coordinates[pointIdx].time,
      ingestEvents
    );

    return (
      <Card className="tooltip-content" style={{ transform: transformStyle }}>
        <Descriptions title={formatDatetime(d.time)} column={1} bordered>
          {points.map((point, i) => (
            <Descriptions.Item label={point.title}>
              <Badge
                color={point.color}
                text={formatStatusAmount(point.data?.error, isPercent)}
              />
            </Descriptions.Item>
          ))}
        </Descriptions>
        <Typography.Text>
          System Version: {records[pointIdx].getSystemVersion()}
        </Typography.Text>
        <Typography.Title level={5}>Ingest Events</Typography.Title>
        {relevantIngestEvents.length > 0 ? (
          <Timeline mode="left">
            {relevantIngestEvents
              .map(({ refreshTimestamp, ingestStatuses }, refreshIdx) => [
                ...ingestStatuses
                  .map((ingestStatus, statusIdx) => {
                    const firstRefresh =
                      refreshIdx === relevantIngestEvents.length - 1;
                    const firstStatusForRefresh =
                      statusIdx === ingestStatuses.length - 1;
                    // If this is not the first refresh, skip the first status.
                    return (!firstRefresh ||
                      ingestStatus.status === IngestStatus.UP_TO_DATE) &&
                      firstStatusForRefresh ? undefined : (
                      <Timeline.Item
                        label={formatDatetime(
                          new Date(ingestStatus.statusTimestamp)
                        )}
                      >
                        {`${
                          firstStatusForRefresh ? "Ongoing " : ""
                        }${getLabelForStatus(ingestStatus.status)}`}
                      </Timeline.Item>
                    );
                  })
                  .reverse(),
                <Timeline.Item
                  label={formatDatetime(new Date(refreshTimestamp))}
                >
                  BigQuery Refresh
                </Timeline.Item>,
              ])
              .reverse()
              .flat()}
          </Timeline>
        ) : (
          <Row justify="center">
            <Typography.Text>No Refreshes</Typography.Text>
          </Row>
        )}
      </Card>
    );
  };
  const margin = { left: 80, bottom: 90, right: 10, top: 40 };
  const width = 700;
  const height = 400;

  const [hoveredAnnotationIdx, setHoveredAnnotationIdx] =
    React.useState<number>(-1);

  const ingestAnnotations = ingestEvents
    .filter(
      // TODO(#20103): If extract and merge is crashing this will still make a bunch of
      // annotations.
      ({ ingestStatuses }) => getLabelForStatuses(ingestStatuses).length > 0
    )
    .map(({ refreshTimestamp, ingestStatuses }) => ({
      time: new Date(refreshTimestamp),
      note: {
        label: getLabelForStatuses(ingestStatuses),
      },
      color: ingestStatuses
        .map((statusInfo) => statusInfo.status)
        .includes(IngestStatus.FLASH_COMPLETED)
        ? palette.data.salmon1
        : palette.data.teal1,
    }));

  const versionAnnotations = Object.entries(versionChanges).map(
    ([version, timestamp]) => ({
      time: timestamp,
      note: {
        label: version,
      },
      color: palette.data.indigo1,
    })
  );

  const annotations = [...ingestAnnotations, ...versionAnnotations].map(
    (annotation, idx) => ({
      type: "x",
      dy: -10,
      dx: 0,
      lineStyle: {
        strokeWidth: 2,
      },
      connector: { type: "line", end: "dot", endScale: 2 },
      events: {
        onMouseOver: () => setHoveredAnnotationIdx(idx),
        onMouseOut: () => setHoveredAnnotationIdx(-1),
      },
      ...annotation,
      // Set the note separately, since we only want to include the label on hover.
      note: {
        align: "middle",
        wrap: 10,
        wrapSlitter: "\n",
        ...annotation.note,
        label: hoveredAnnotationIdx === idx ? annotation.note.label : undefined,
      },
    })
  );

  return (
    <ChartWrapper className="validation-graph">
      {loading ? <Spin style={{ position: "absolute" }} /> : undefined}
      <XYFrame
        annotations={loading ? undefined : annotations}
        axes={[
          {
            orient: "left",
            tickFormat: (amount: number) =>
              formatStatusAmount(amount, isPercent),
          },
          {
            orient: "bottom",
            tickFormat: (time: Date) => formatDate(time),
            ticks: 5,
            tickSize: 0,
          },
        ]}
        lines={loading ? undefined : lines}
        lineStyle={(d: { color: string }, i: number) => ({
          stroke: d.color,
          strokeWidth: 2,
        })}
        size={[width, height]}
        margin={margin}
        xAccessor="time"
        yAccessor="error"
        // @ts-expect-error Semiotic typedefs are wrong, we can
        // use a time scale here, not just a numeric one.
        xScaleType={scaleTime()}
        yExtent={[0, yMax]}
        hoverAnnotation={[
          { type: "x", disable: ["connector", "note"] },
          { type: "frame-hover" },
        ]}
        tooltipContent={tooltipContent}
      />
      <Space split={<Divider type="vertical" />}>
        <ColorLegend items={lines} />
        {ingestAnnotations.length > 0 ? (
          <ColorLegend
            items={[
              {
                title: "Refresh with Flashed Data",
                color: palette.data.salmon1,
              },
              {
                title: "Refresh with Ingested Data",
                color: palette.data.teal1,
              },
            ]}
          />
        ) : undefined}
        {versionAnnotations.length > 0 ? (
          <ColorLegend
            items={[
              {
                title: "Version Update",
                color: palette.data.indigo1,
              },
            ]}
          />
        ) : undefined}
      </Space>
    </ChartWrapper>
  );
};

export default ValidationDetailsGraph;
