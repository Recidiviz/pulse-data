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

import fs from "fs";
import yargs from "yargs";
import { parse } from "csv-parse";
import { deleteCollection, getDb } from "./firestoreUtils.js";
import _ from "lodash";
import { parseISO } from "date-fns";

const argv = yargs(process.argv.slice(2))
  .option("file", {
    alias: "f",
    description: "path to the CSV containing the case data",
    demandOption: true,
    type: "string",
    normalize: true,
  })
  .help()
  .alias("help", "h").argv;

const COLLECTIONS = {
  compliantReportingCases: "compliantReportingCases",
};

function camelCaseKeys(obj) {
  return _.mapKeys(obj, (v, k) => _.camelCase(k));
}

function titleCase(input) {
  return _.startCase(_.lowerCase(input));
};

const db = getDb();

console.log("wiping existing data ...");
await deleteCollection(db, COLLECTIONS.compliantReportingCases);

console.log("loading new data...");
const bulkWriter = db.bulkWriter();
// Iterate through each record
const parser = fs.createReadStream(argv.file).pipe(parse({ columns: true }));
for await (const record of parser) {
  const docData = camelCaseKeys(record);

  // transform complex fields
  const lastDrun = _.trim(docData.lastDrun, "[]").split(",").map(parseISO);

  const nameParts = JSON.parse(docData.personName);
  const personName = `${titleCase(nameParts.given_names)} ${titleCase(
    nameParts.surname
  )}`;

  const supervisionLevelStart = parseISO(docData.supervisionLevelStart);

  const supervisionType = titleCase(docData.supervisionType);

  bulkWriter.create(db.collection(COLLECTIONS.compliantReportingCases).doc(), {
    ...docData,
    lastDrun,
    supervisionLevelStart,
    personName,
    supervisionType,
  });
}

bulkWriter
  .close()
  .then(() =>
    console.log("new compliant reporting case data loaded successfully")
  );
