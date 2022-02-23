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

import "react-datepicker/dist/react-datepicker.css";

import { fonts, palette } from "@recidiviz/design-system";
import React, { forwardRef } from "react";
import DatePicker from "react-datepicker";
import styled from "styled-components/macro";

import { UpcomingDischargeCase } from "../DataStores/CaseStore";
import DischargeTooltip from "../DischargeTooltip";
import DisplayDate from "../DisplayDate";

const DatePickerContainer = styled.div`
  display: inline-block;

  .calendar-date-picker__display-button {
    background: none;
    border: none;
    color: ${palette.slate80};
    cursor: pointer;
    padding: 0;
    text-decoration: underline;

    &:hover {
      color: ${palette.signal.links};
    }
  }

  .react-datepicker {
    background: ${palette.marble1};
    border-color: ${palette.slate60};
    font-family: ${fonts.sans};

    &__header {
      background: none;
      border-bottom: none;
      padding-bottom: 0;
    }

    &__current-month {
      color: ${palette.pine3};
    }

    &__navigation-icon::before {
      color: ${palette.slate60};
      border-width: 1.5px 1.5px 0 0;
    }

    &__day-name {
      color: ${palette.slate60};
    }

    &__day {
      border: 1px solid ${palette.marble1};
      &--outside-month {
        color: ${palette.slate60};
      }

      &--selected {
        background: ${palette.pine4};
        color: ${palette.marble1};

        &:hover {
          background: ${palette.pine4} !important;
          border-color: ${palette.marble1} !important;
          color: ${palette.marble1};
        }
      }

      &--keyboard-selected {
        background: ${palette.signal.highlight};
        color: ${palette.marble1};

        &:hover {
          background: ${palette.signal.highlight} !important;
          border-color: ${palette.marble1} !important;
          color: ${palette.marble1};
        }
      }

      &:hover {
        background: none;
        border-color: ${palette.pine2};
      }
    }
  }
`;

type DatePickerProps = {
  onPickDate: (date: Date | null) => void;
  currentDate: Date | null;
  placeholderText: string;
  startOpen?: boolean;
  onClose?: () => void;
  record?: UpcomingDischargeCase;
};

const CalendarDatePicker: React.FC<DatePickerProps> = ({
  onPickDate,
  currentDate,
  placeholderText,
  startOpen = false,
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  onClose = () => {},
  record,
}) => {
  const DisplayDateElement = forwardRef<HTMLButtonElement>((props, ref) => (
    <DischargeTooltip record={record}>
      <button
        ref={ref}
        // @ts-ignore
        onClick={props.onClick}
        className="calendar-date-picker__display-button"
        type="button"
      >
        {currentDate ? <DisplayDate date={currentDate} /> : placeholderText}
      </button>
    </DischargeTooltip>
  ));

  return (
    <DatePickerContainer>
      <DatePicker
        startOpen={startOpen}
        onChange={onPickDate}
        onCalendarClose={onClose}
        selected={currentDate}
        customInput={<DisplayDateElement />}
      />
    </DatePickerContainer>
  );
};

export default CalendarDatePicker;
