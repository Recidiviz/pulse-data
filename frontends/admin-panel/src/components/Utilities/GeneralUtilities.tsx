// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2025 Recidiviz, Inc.
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
export function optionalStringSort(
  a: string | null | undefined,
  b: string | null | undefined
): number {
  return a?.localeCompare(b || "") || -1;
}

export function optionalNumberSort(
  a: number | null | undefined,
  b: number | null | undefined
): number {
  return (a || 0) - (b || 0);
}

export function booleanSort(a: boolean, b: boolean): number {
  const aNumber = a ? 1 : 0;
  const bNumber = b ? 1 : 0;
  return aNumber - bNumber;
}

export function optionalBooleanSort(
  a: boolean | null | undefined,
  b: boolean | null | undefined
): number {
  const aIsBoolean = typeof a === "boolean";
  const bIsBoolean = typeof b === "boolean";
  return aIsBoolean && bIsBoolean
    ? booleanSort(a as boolean, b as boolean)
    : booleanSort(aIsBoolean, bIsBoolean);
}

export function getUniqueValues<T>(values: T[]): T[] {
  return Array.from(new Set(values));
}

/**
 * Scrolls to the first element with the given hash
 * @param hash id to scroll to
 */
export function scrollToAnchor(hash: string): void {
  const id = hash.replace("#", "");

  if (id) {
    const anchor = document.getElementById(id);
    if (anchor) {
      anchor.scrollIntoView();
      anchor.classList.add(...["highlight"]);
      setTimeout(() => anchor.classList.remove(...["highlight"]), 1000);
    }
  }
}

export const formatDatetime = (date?: Date): string | undefined => {
  return date?.toLocaleString("en-US", { timeZoneName: "short" });
};

export const formatDatetimeFromTimestamp = (
  timestampInSeconds?: number
): string | undefined => {
  if (timestampInSeconds) {
    // convert seconds to milliseconds
    return formatDatetime(new Date(timestampInSeconds * 1000));
  }
  return undefined;
};

// taken from https://stackoverflow.com/questions/42998927/accessibility-react-ensure-click-events-have-key-events
export function buttonizeWithEvent(
  handler: (event: React.KeyboardEvent<HTMLDivElement>) => void
): object {
  return {
    role: "button",
    onClick: handler,
    onKeyDown: (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (event.key === "Enter") handler(event);
    },
  };
}

export function buttonize(handler: () => void): object {
  return {
    role: "button",
    onClick: handler,
    onKeyDown: (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (event.key === "Enter") handler();
    },
  };
}
