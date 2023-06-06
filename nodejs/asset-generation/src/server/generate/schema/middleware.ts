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

import { NextFunction, Request, Response } from "express";
import { infer as ZodInfer, ZodTypeAny } from "zod";
import { fromZodError } from "zod-validation-error";

import { ValidatedInput } from "../types";

export function schemaMiddleware<Schema extends ZodTypeAny>(schema: Schema) {
  return function (
    req: Request,
    // note that these types reflect what this function may ADD to the response,
    // not what we expect to already be there
    res: Response<{ error: string }, ValidatedInput<ZodInfer<Schema>>>,
    next: NextFunction
  ) {
    const result = schema.safeParse(req.body);
    if (!result.success) {
      res.status(400).json({ error: fromZodError(result.error).message });
    } else {
      res.locals.data = result.data;
      next();
    }
  };
}
