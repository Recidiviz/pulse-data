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

import React, { useState } from "react";

export type FormContexts = {
  [contextKey: string]: string | number;
};

export type FormDimensions = {
  [dimensionKey: string]: string | number;
};

export type FormDisaggregations = {
  [disaggregationKey: string]: FormDimensions;
};

export type FormMetric = {
  value: string | number;
  contexts: FormContexts;
  disaggregations: FormDisaggregations;
};

export type FormReport = {
  [metricID: string]: FormMetric;
};

interface UseFormProps {
  initialValues?: FormReport;
  validations?: Record<string, unknown>;
  onSubmit?: () => void;
}

export const useForm = (options: UseFormProps) => {
  const [formData, setFormData] = useState(
    (options?.initialValues || {}) as FormReport
  );

  const handleMetricChange = (
    metricID: string,
    e: React.ChangeEvent<HTMLInputElement>
  ) => {
    setFormData((prev) => {
      return {
        ...prev,
        [metricID]: {
          ...prev[metricID],
          value: e.target.value,
        },
      };
    });
  };

  const handleContextChange = (
    metricID: string,
    e: React.ChangeEvent<HTMLInputElement>
  ) => {
    setFormData((prev) => {
      return {
        ...prev,
        [metricID]: {
          ...prev[metricID],
          contexts: {
            ...prev[metricID].contexts,
            [e.target.name]: e.target.value,
          },
        },
      };
    });
  };

  const handleDisaggregationDimensionChange = (
    metricID: string,
    disaggregationKey: string,
    e: React.ChangeEvent<HTMLInputElement>
  ) => {
    setFormData((prev) => {
      return {
        ...prev,
        [metricID]: {
          ...prev[metricID],
          disaggregations: {
            ...prev[metricID].disaggregations,
            [disaggregationKey]: {
              ...prev[metricID].disaggregations[disaggregationKey],
              [e.target.name]: e.target.value,
            },
          },
        },
      };
    });
  };

  const resetBinaryInputs = (
    metricID: string,
    e: React.MouseEvent<HTMLDivElement>
  ) => {
    const fieldKey = e.currentTarget.dataset.name as string;
    setFormData((prev) => {
      return {
        ...prev,
        [metricID]: {
          ...prev[metricID],
          contexts: {
            ...prev[metricID].contexts,
            [fieldKey]:
              options?.initialValues?.[metricID].contexts[fieldKey] || "",
          },
        },
      };
    });
  };

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();

    if (options?.onSubmit) {
      options.onSubmit();
    }
  };

  return {
    formData,
    handleMetricChange,
    handleContextChange,
    handleDisaggregationDimensionChange,
    handleSubmit,
    resetBinaryInputs,
  };
};
