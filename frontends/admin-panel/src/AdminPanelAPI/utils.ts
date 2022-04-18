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
export const getResource = async (url: string): Promise<Response> => {
  return fetch(`/admin${url}`, {
    headers: {
      "Content-Type": "application/json",
    },
  });
};

export const postWithURLAndBody = async (
  url: string,
  body: Record<string, unknown> = {}
): Promise<Response> => {
  return fetch(`/admin${url}`, {
    method: "POST",
    body: JSON.stringify(body),
    headers: {
      "Content-Type": "application/json",
    },
  });
};

export const putWithURLAndBody = async (
  url: string,
  body: Record<string, unknown> = {}
): Promise<Response> => {
  return fetch(`/admin${url}`, {
    method: "PUT",
    body: JSON.stringify(body),
    headers: {
      "Content-Type": "application/json",
    },
  });
};
