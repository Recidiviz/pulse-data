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

export const useToggleFocus = <
  FirstRefElement extends HTMLElement,
  SecondRefElement extends HTMLElement
>(): [
  boolean | undefined,
  React.Dispatch<React.SetStateAction<boolean | undefined>>,
  React.Ref<FirstRefElement>,
  React.Ref<SecondRefElement>
] => {
  // Hook that allows users to toggle between two elements while maintaining focus
  const [isToggled, setIsToggled] = React.useState<boolean | undefined>(false);
  const previouslyToggledRef = React.useRef<boolean | undefined>();

  const firstRef = React.useRef<FirstRefElement>(null);
  const secondRef = React.useRef<SecondRefElement>(null);

  React.useEffect(() => {
    const { current: wasPreviouslyToggled } = previouslyToggledRef;
    if (wasPreviouslyToggled === false && isToggled) {
      secondRef.current?.focus();
    }
    if (wasPreviouslyToggled === true && !isToggled) {
      firstRef.current?.focus();
    }
  }, [isToggled, firstRef, secondRef]);

  React.useEffect(() => {
    previouslyToggledRef.current = isToggled;
  });

  return [isToggled, setIsToggled, firstRef, secondRef];
};
