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

import { mapValues } from "lodash";
import { observer } from "mobx-react-lite";
import React, { useEffect } from "react";

import {
  Datapoint,
  DatapointsGroupedByAggregateAndDisaggregations,
  DatapointsViewSetting,
  DataVizTimeRangesMap,
} from "../../shared/types";
import { useStore } from "../../stores";
import BarChart from "./BarChart";
import {
  DatapointsViewContainer,
  DatapointsViewControlsContainer,
  DatapointsViewControlsDropdown,
} from "./DatapointsView.styles";
import Legend from "./Legend";
import {
  createGMTDate,
  getDatapointDimensions,
  getHighestTotalValue,
  getSumOfDimensionValues,
  incrementMonth,
  incrementYear,
  sortDatapointDimensions,
  thirtyOneDaysInSeconds,
  threeHundredSixtySixDaysInSeconds,
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
    (selectedDisaggregation &&
      !!datapointsForMetric?.disaggregations[selectedDisaggregation] &&
      Object.values(
        datapointsForMetric.disaggregations[selectedDisaggregation]
      )) ||
    datapointsForMetric?.aggregate ||
    [];
  const isAnnual = data[0]?.frequency === "ANNUAL";
  const disaggregationOptions =
    (datapointsForMetric?.disaggregations &&
      Object.keys(datapointsForMetric.disaggregations)) ||
    [];
  disaggregationOptions.unshift(noDisaggregationOption);

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
  }, [metric, selectedDisaggregation]);

  const renderChartForMetric = () => {
    return (
      <BarChart
        data={transformData(
          data,
          selectedTimeRangeValue,
          datapointsViewSetting
        )}
        percentageView={
          !!selectedDisaggregation && datapointsViewSetting === "Percentage"
        }
      />
    );
  };

  const renderLegend = () => {
    if (datapointsForMetric?.disaggregations[selectedDisaggregation]) {
      const datapoint = Object.values(
        datapointsForMetric.disaggregations[selectedDisaggregation]
      )?.[0];
      if (datapoint) {
        const dimensionNames = Object.keys(
          getDatapointDimensions(datapoint)
        ).sort(sortDatapointDimensions);
        return <Legend names={dimensionNames} />;
      }
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
        <DatapointsViewControlsDropdown
          title="Disaggregation"
          selectedValue={selectedDisaggregation}
          options={disaggregationOptions}
          onSelect={(key) => {
            setSelectedDisaggregation(key);
          }}
        />
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

  return (
    <DatapointsViewContainer>
      {renderDataVizControls()}
      {renderChartForMetric()}
      {renderLegend()}
    </DatapointsViewContainer>
  );
};

export default observer(DatapointsView);
