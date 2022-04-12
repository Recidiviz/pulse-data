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

// separate multiple people on a list by comma - no comma for the last person on the list
// e.g. ['Editor 1', 'Editor 2', 'Editor 3'] would print: `Editor 1, Editor 2, Editor 3`
export const printCommaSeparatedList = (list: string[]): string => {
  const string = list.map((item, i) =>
    i < list.length - 1 ? `${item}, ` : `${item}`
  );
  return string.join(" ");
};
