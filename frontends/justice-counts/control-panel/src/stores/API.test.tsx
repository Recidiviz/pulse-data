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
import API from "./API";

const mockFetch = fetch as jest.Mock;
const MockAuthStore = jest.fn(() => {
  return {
    getToken: () => "token",
  };
}) as jest.Mock;

afterEach(() => {
  jest.resetAllMocks();
});

describe("testing API calls", () => {
  const api = new API(MockAuthStore());

  test("error calling protected api", async () => {
    mockFetch.mockRejectedValue(new Error("Failed to fetch"));

    await expect(
      api.request({
        path: "/api/hello",
        method: "GET",
      })
    ).rejects.toThrow("Failed to fetch");
    expect(fetch).toBeCalledTimes(1);
  });

  test("successfully calling protected api", async () => {
    mockFetch.mockResolvedValue({
      json: () =>
        Promise.resolve({
          message: "The API successfully validated your access token.",
        }),
    });

    const response = (await api.request({
      path: "/api",
      method: "GET",
    })) as Response;
    const data = await response.json();

    expect(data.message).toBe(
      "The API successfully validated your access token."
    );
    expect(fetch).toBeCalledTimes(1);

    expect.hasAssertions();
  });

  test("call to protected api has authorization header", async () => {
    await expect(api.request({ path: "/api", method: "POST" })).rejects.toThrow(
      Error
    );
    expect(mockFetch.mock.calls[0][1].headers).toHaveProperty("Authorization");

    expect.hasAssertions();
  });
});
