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
import * as React from "react";
import { ReactNode } from "react";
import { useField } from "formik";
import { CheckboxContainer, Checkbox, CheckedIcon } from "./Checkbox.styles";

interface CheckboxComponentProps {
  children: ReactNode | ReactNode[];
  className?: string;
  name: string;
  value: string;
}

const CheckboxComponent: React.FC<CheckboxComponentProps> = ({
  children,
  className,
  name,
  value,
}: CheckboxComponentProps) => {
  const syntheticElement = { name, type: "checkbox", value };
  const [field] = useField(syntheticElement);

  return (
    <CheckboxContainer
      className={className}
      aria-checked={field.checked}
      onKeyDown={(event) => {
        if (event.key === " ") {
          event.preventDefault();
          field.onChange({
            target: { ...syntheticElement, checked: !field.checked },
          });
        }
      }}
      onClick={(event) => {
        event.preventDefault();
        field.onChange({
          target: { ...syntheticElement, checked: !field.checked },
        });
      }}
      onBlur={(event) => field.onBlur({ target: syntheticElement })}
    >
      <Checkbox checked={field.checked}>
        <CheckedIcon />
      </Checkbox>
      {children}
    </CheckboxContainer>
  );
};

CheckboxComponent.defaultProps = {
  className: "",
};

export default CheckboxComponent;
