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
  compliantReportingReferrals: "compliantReportingReferrals",
};

function camelCaseKeys(obj) {
  return _.mapKeys(obj, (v, k) => _.camelCase(k));
}

const db = getDb();

console.log("wiping existing data ...");
await deleteCollection(db, COLLECTIONS.compliantReportingReferrals);

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
  "MISDEMEANOR PROBATIONER": "Misdemeanor Probation",
};

// Iterate through each record
rawCases.forEach((record) => {
  const docData = camelCaseKeys(record);

  const offenseType = JSON.parse(docData.offenseType);

  const supervisionType =
    SUPERVISION_TYPE_MAPPING[docData.supervisionType] ??
    docData.supervisionType;

  bulkWriter.create(db.collection(COLLECTIONS.compliantReportingReferrals).doc(), {
    ...docData,
    offenseType,
      supervisionType,
  });
});

bulkWriter
  .close()
  .then(() =>
    console.log("new compliant reporting referral data loaded successfully")
  );
