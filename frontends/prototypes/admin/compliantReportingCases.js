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
import { deleteCollection, getDb } from "./firestoreUtils.js";
import _ from "lodash";
import { parseISO } from "date-fns";
import { titleCase } from "title-case";

const argv = yargs(process.argv.slice(2))
  .option("file", {
    alias: "f",
    description: "path to the JSON file containing the case data",
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

const db = getDb();

console.log("wiping existing data ...");
await deleteCollection(db, COLLECTIONS.compliantReportingCases);

console.log("loading new data...");
const bulkWriter = db.bulkWriter();

// read in the JSON file (it shouldn't be too big, maybe a few  MB)
const rawCases = JSON.parse(fs.readFileSync(argv.file), "utf8");

// cleaner display values for raw TN values
const SUPERVISION_TYPE_MAPPING = {
  DIVERSION: "Diversion",
  "TN PROBATIONER": "Probation",
  "TN PAROLEE": "Parole",
  "ISC FROM OTHER JURISDICTION": "ISC",
  "DETERMINATE RLSE PROBATIONER": "Determinate Release Probation",
  "SPCL ALT INCARCERATION UNIT": "SAIU",
  "MISDEMEANOR PROBATIONER": "Misdemeaner Probation",
};

// Iterate through each record
rawCases.forEach((record) => {
  if (record.district !== 'DISTRICT 50') return;
  
  const docData = camelCaseKeys(record);

  // transform complex fields
  const lastDrun = docData.lastDrun.map(parseISO);

  const nameParts = JSON.parse(docData.personName);
  const personName = titleCase(`${nameParts.given_names} ${nameParts.surname}`.toLowerCase());

  const supervisionLevelStart = parseISO(docData.supervisionLevelStart);

  const supervisionType =
    SUPERVISION_TYPE_MAPPING[docData.supervisionType] ??
    docData.supervisionType;
  const supervisionLevel = titleCase(docData.supervisionLevel.toLowerCase());

  bulkWriter.create(db.collection(COLLECTIONS.compliantReportingCases).doc(), {
    ...docData,
    lastDrun,
    supervisionLevelStart,
    personName,
    nameParts,
    supervisionType,
    supervisionLevel,
  });
});

bulkWriter
  .close()
  .then(() =>
    console.log("new compliant reporting case data loaded successfully")
  );
