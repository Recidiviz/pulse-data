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

const argv = yargs(process.argv.slice(2))
  .option("file", {
    alias: "f",
    description: "path to the CSV containing the user mapping",
    demandOption: true,
    type: "string",
    normalize: true,
  })
  .help()
  .alias("help", "h").argv;

const COLLECTIONS = {
  // indexed by email address to connect with Auth0
  users: "users",
  // indexed by officer ID to connect with ingested data
  officers: "officers",
};

const db = getDb();

console.log("wiping existing collections ...");
await deleteCollection(db, COLLECTIONS.users);
await deleteCollection(db, COLLECTIONS.officers);

console.log("loading new data...");
const bulkWriter = db.bulkWriter();
// Iterate through each record
const parser = fs.createReadStream(argv.file).pipe(parse({ columns: true }));
for await (const record of parser) {
  const officerIds = record.officer_ids.split(",").map((id) => id.trim());
  const name = record["User name"].trim();
  const email = record["Email"].toLowerCase().trim();

  // entries with an email will have user accounts
  if (email.includes("@")) {
    bulkWriter.create(db.doc(`${COLLECTIONS.users}/${email}`), {
      name,
      officerIds,
    });
  }

  // entries that map to a single ID will have cases
  if (officerIds.length === 1) {
    bulkWriter.create(db.doc(`${COLLECTIONS.officers}/${officerIds[0]}`), {
      name,
    });
  }
}

bulkWriter.close().then(() => console.log("new user data loaded successfully"));
