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
import { User } from "../types";

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

export const deleteWithUrlAndBody = async (
  url: string,
  body?: Record<string, unknown>
): Promise<Response> => {
  return fetch(`/admin${url}`, {
    method: "DELETE",
    body: JSON.stringify(body),
    headers: {
      "Content-Type": "application/json",
    },
  });
};

export const patchWithURLAndBody = async (
  url: string,
  body: Record<string, unknown> = {}
): Promise<Response> => {
  return fetch(`/admin${url}`, {
    method: "PATCH",
    body: JSON.stringify(body),
    headers: {
      "Content-Type": "application/json",
    },
  });
};

export const getAuthResource = async (url: string): Promise<Response> => {
  return fetch(`/auth${url}`, {
    headers: {
      "Content-Type": "application/json",
    },
  });
};

export const postAuthWithURLAndBody = async (
  url: string,
  body: Record<string, unknown> | User[]
): Promise<Response> => {
  return fetch(`/auth${url}`, {
    method: "POST",
    body: JSON.stringify(body),
    headers: {
      "Content-Type": "application/json",
    },
  });
};

export const patchAuthWithURLAndBody = async (
  url: string,
  body: Record<string, unknown> | Record<string, unknown>[] = {}
): Promise<Response> => {
  return fetch(`/auth${url}`, {
    method: "PATCH",
    body: JSON.stringify(body),
    headers: {
      "Content-Type": "application/json",
    },
  });
};

export const putAuthWithURLAndBody = async (
  url: string,
  body: Record<string, unknown> = {}
): Promise<Response> => {
  return fetch(`/auth${url}`, {
    method: "PUT",
    body: JSON.stringify(body),
    headers: {
      "Content-Type": "application/json",
    },
  });
};

export const deleteResource = async (
  url: string,
  body?: Record<string, unknown>
): Promise<Response> => {
  return fetch(`/auth${url}`, {
    method: "DELETE",
    headers: {
      "Content-Type": "application/json",
    },
    ...(body && { body: JSON.stringify(body) }),
  });
};

interface RequestProps {
  path: string;
  method: "GET" | "POST" | "PATCH" | "DELETE" | "PUT";
  body?: Record<string, unknown>;
  retrying?: boolean;
}

/**
 * API functions with error handling
 */
/* eslint-disable @typescript-eslint/no-explicit-any */
export const request = async ({
  path,
  method,
  body,
  retrying = false,
}: RequestProps): Promise<any> => {
  const headers: HeadersInit = {
    "Content-Type": "application/json",
  };

  const response = await fetch(path, {
    body: method !== "GET" ? JSON.stringify(body) : null,
    method,
    headers,
  });

  const json = await response.json();

  if (!response.ok) {
    throw new Error(
      `API request failed.
      \nStatus: ${response.status} - ${response.statusText}
      \nErrors: ${JSON.stringify(json.errors ?? json)}`
    );
  }
  return json;
};

export const get = async (
  path: string,
  body: Record<string, string> = {}
): Promise<any> => {
  return request({ path, body, method: "GET" });
};

export const post = async (
  path: string,
  body: Record<string, unknown> = {}
): Promise<any> => {
  return request({ path, body, method: "POST" });
};

export const put = async (
  path: string,
  body: Record<string, unknown> = {}
): Promise<any> => {
  return request({ path, body, method: "PUT" });
};

export const patch = async (
  path: string,
  body: Record<string, unknown> = {}
): Promise<any> => {
  return request({ path, body, method: "PATCH" });
};
/* eslint-enable @typescript-eslint/no-explicit-any */
