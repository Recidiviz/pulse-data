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

import { observer } from "mobx-react-lite";
import React, { useEffect } from "react";

import {
  DatapointsGroupedByAggregateAndDisaggregations,
  DatapointsViewSetting,
  DataVizAggregateName,
  DataVizTimeRangesMap,
} from "../../shared/types";
import { useStore } from "../../stores";
import BarChart from "./BarChart";
import {
  DatapointsViewContainer,
  DatapointsViewControlsContainer,
  DatapointsViewControlsDropdown,
  MetricInsight,
  MetricInsightsRow,
} from "./DatapointsView.styles";
import Legend from "./Legend";
import {
  filterByTimeRange,
  filterNullDatapoints,
  getAverageTotalValue,
  getLatestDateFormatted,
  getPercentChangeOverTime,
  sortDatapointDimensions,
  transformData,
} from "./utils";

const noDisaggregationOption = "None";

const DatapointsView: React.FC<{
  metric: string;
}> = ({ metric }) => {
  const { datapointsStore } = useStore();
  const [selectedTimeRange, setSelectedTimeRange] =
    React.useState<string>("All");
  const [selectedDisaggregation, setSelectedDisaggregation] =
    React.useState<string>(noDisaggregationOption);
  const [datapointsViewSetting, setDatapointsViewSetting] =
    React.useState<DatapointsViewSetting>("Count");

  const datapointsForMetric = datapointsStore.datapointsByMetric[
    metric
  ] as DatapointsGroupedByAggregateAndDisaggregations;
  const data =
    (selectedDisaggregation !== noDisaggregationOption &&
      Object.values(
        datapointsForMetric.disaggregations[selectedDisaggregation] || {}
      )) ||
    datapointsForMetric?.aggregate ||
    [];
  const isAnnual = data[0]?.frequency === "ANNUAL";
  const disaggregationOptions = Object.keys(
    datapointsStore.dimensionNamesByMetricAndDisaggregation[metric] || {}
  );
  disaggregationOptions.unshift(noDisaggregationOption);
  const dimensionNames =
    selectedDisaggregation !== noDisaggregationOption
      ? (
          datapointsStore.dimensionNamesByMetricAndDisaggregation[metric]?.[
            selectedDisaggregation
          ] || []
        )
          .slice() // Must use slice() before sorting a MobX observableArray
          .sort(sortDatapointDimensions)
      : [DataVizAggregateName];

  const selectedTimeRangeValue = DataVizTimeRangesMap[selectedTimeRange];

  useEffect(() => {
    if (isAnnual && selectedTimeRangeValue === 6) {
      setSelectedTimeRange("All");
    }
    if (!disaggregationOptions.includes(selectedDisaggregation)) {
      setSelectedDisaggregation(noDisaggregationOption);
      setDatapointsViewSetting("Count");
    }
    if (selectedDisaggregation === noDisaggregationOption) {
      setDatapointsViewSetting("Count");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [metric]);

  const renderChartForMetric = () => {
    return (
      <BarChart
        data={transformData(
          data,
          selectedTimeRangeValue,
          datapointsViewSetting
        )}
        dimensionNames={dimensionNames}
        percentageView={
          !!selectedDisaggregation && datapointsViewSetting === "Percentage"
        }
      />
    );
  };

  const renderLegend = () => {
    if (selectedDisaggregation !== noDisaggregationOption) {
      return <Legend names={dimensionNames} />;
    }
    return <Legend />;
  };

  const renderDataVizControls = () => {
    return (
      <DatapointsViewControlsContainer>
        <DatapointsViewControlsDropdown
          title="Date Range"
          selectedValue={selectedTimeRange}
          options={
            isAnnual
              ? Object.keys(DataVizTimeRangesMap).filter(
                  (key) => key !== "6 Months Ago"
                )
              : Object.keys(DataVizTimeRangesMap)
          }
          onSelect={(key) => {
            setSelectedTimeRange(key);
          }}
        />
        {disaggregationOptions.length > 1 && (
          <DatapointsViewControlsDropdown
            title="Disaggregation"
            selectedValue={selectedDisaggregation}
            options={disaggregationOptions}
            onSelect={(key) => {
              setSelectedDisaggregation(key);
            }}
          />
        )}
        {selectedDisaggregation !== noDisaggregationOption && (
          <DatapointsViewControlsDropdown
            title="View"
            selectedValue={datapointsViewSetting}
            options={["Count", "Percentage"]}
            onSelect={(key) => {
              setDatapointsViewSetting(key as DatapointsViewSetting);
            }}
          />
        )}
      </DatapointsViewControlsContainer>
    );
  };

  const renderMetricInsightsRow = () => {
    const dataSelectedInTimeRange = filterNullDatapoints(
      filterByTimeRange(
        datapointsForMetric?.aggregate || [],
        selectedTimeRangeValue
      )
    );
    const percentChange = getPercentChangeOverTime(dataSelectedInTimeRange);
    const avgValue = getAverageTotalValue(dataSelectedInTimeRange, isAnnual);

    return (
      <MetricInsightsRow>
        <MetricInsight title="% Total Change" value={percentChange} />
        <MetricInsight title="Avg. Total Value" value={avgValue} />
        <MetricInsight
          title="Most Recent"
          value={getLatestDateFormatted(dataSelectedInTimeRange, isAnnual)}
        />
      </MetricInsightsRow>
    );
  };

  return (
    <DatapointsViewContainer>
      {renderDataVizControls()}
      {renderMetricInsightsRow()}
      {renderChartForMetric()}
      {renderLegend()}
    </DatapointsViewContainer>
  );
};

export default observer(DatapointsView);
