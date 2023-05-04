// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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

import { expect, test, vi } from "vitest";
import { z } from "zod";

import { schemaMiddleware } from "./schemaMiddleware";

const testSchema = z.object({
  foo: z.string(),
});

test("valid input", () => {
  const req = {
    body: { foo: "bar" },
  };
  const res = { locals: {} };
  const next = vi.fn();
  const middleware = schemaMiddleware(testSchema);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  middleware(req as any, res as any, next);

  expect(next).toHaveBeenCalled();
  expect(res.locals).toEqual({ data: { foo: "bar" } });
});

test("invalid input", () => {
  const req = {
    body: { foo: 7 },
  };

  const res = { status: vi.fn(), json: vi.fn() };
  res.status.mockReturnValue(res);
  res.json.mockReturnValue(res);

  const next = vi.fn();

  const middleware = schemaMiddleware(testSchema);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  middleware(req as any, res as any, next);

  expect(next).not.toHaveBeenCalled();
  expect(res.status).toHaveBeenCalledWith(400);
  expect(res.json.mock.calls[0]).toMatchInlineSnapshot(`
    [
      {
        "error": "Validation error: Expected string, received number at \\"foo\\"",
      },
    ]
  `);
});
