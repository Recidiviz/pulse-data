# Database

## Overview

The database is a vanilla SQL database with application-time temporal tables.

### Temporal tables

An application-time temporal table is a table that tracks the changes over time to an entity reflected in another table (the "master table"). E.g., `person_history` tracks the changes over time to a given person in the `person` table.

Each row in the temporal table (a "snapshot") contains the state of that entity for a certain period of time, along with `valid_from` and `valid_to` timestamp columns defining the period over which that state is valid. The most recent snapshot is always identical to the current state of the entity on the master table, and has a `valid_to` value of `NULL`.

These tables are designed to be queried both vertically (the complete state of the database, including all relationships, at a given historical point in time) and horizontally (the changes to one entity over time).

The valid period columns reflect the state of the entity in the **real world** (to our best approximation), **not** the state of the entity in the database. E.g., if historical data for a sentence that was imposed in 2005 are ingested into the system, the corresponding historical snapshot will be dated to 2005 (the time the event happened in the real world), not the time the data were recorded in the database.

As part of the temporal table design, no rows are ever deleted from the master tables. Any event corresponding to a delete is instead indicated by updating a status value. This makes it easier to keep track of all entities that might ever have been related to a given entity at any point in its history, even if some of those entities are no longer valid at some later date.

#### Determining valid periods

As a complication to the above, the vast majority of ingested data does not provide sufficient detail for us to ascertain the real-world time that the state of any given entity changed. Because of this, for all updates to existing entities, we use the time the update was ingested and treat it as the time the update occurred. It is only when ingesting new entities that we use whatever provided times are available to determine the valid period of the data.

The most important time considered when ingesting new entities is the booking admission date. When a new booking is ingested, if its provided admission date is earlier than the ingestion date, the booking, as well as all of its children that do not provide more granular dates, are treated as valid from the provided admission date. Additionally, if the booking is the first booking for the given person, that person will also have their valid period dated from the provided admission date.

Note this only applies to new bookings. A new bond, for example, added to an existing booking will have its valid period dated from ingestion time, not the booking's admission date.

To see which descendants of booking provide more granular dates for setting their valid periods, see the `BOOKING_DESCENDANT_START_DATE_FIELD` and `BOOKING_DESCENDANT_END_DATE_FIELD` maps in `update_historical_snapshots.py`.

## Querying

Do not query `prod-data` directly. `prod-data` should only be queried to validate migrations, as detailed below. All other queries should be run against the BigQuery export of `prod-data`.

For more information on querying from BigQuery, check the [wiki page](https://github.com/Recidiviz/pulse-data/wiki/Querying-data).

## Migrations

### Migration warnings and constraints

- Do not make any changes to `schema.py` without also running a migration to update the database. All jobs will fail for any release in which `schema.py` does not match the database.

- A migration should consist of a single change to the schema (a single change meaning a single conceptual change, which may consist of multiple update statements). Do not group changes.

- For historical tables, the primary key exists only due to requirements of SQLAlchemy, and should never be referenced elsewhere in the schema.

- If adding a column of type String, the length must be specified (this keeps the schema portable, as certain SQL implementations require an explicit length for String columns).

- Do not explicitly set the column name argument for a column unless it is required to give a column the same name as a Python reserved keyword.

### Running migrations

All migrations should be run from the `prod-data-client` VM, which is accessed by `gcloud compute ssh prod-data-client`. Then follow the steps below according to the type of migration.

If it is your **first time logging in to the VM**, you will need to set a password when you run ssh. You will then be prompted to re-enter the same password to access the VM. Once you have successfully logged in, run `initial-setup` from your home directory. This will set up the git repository, pip environment, and SSL certificates you will need to run migrations.

NOTE: All commands below assume you are **running in your pip environment**. To launch it, run `pipenv shell` from the **top-level package of `pulse-data`** (not your home directory).

#### Generating the migration

All the below options for generating a migration require you to specify a migration name. Use the same naming style as a Git branch, e.g. `add_bond_id_column`.

NOTE: There is a strange behavior that sometimes occurs with enums and auto-generated migrations. Typically, alembic will not detect changes to an enum like adding or removing a value, which is why the `create_enum_migration` script is used. However, sometimes alembic will detect those changes, and will include statements to update the enum values in the auto-generated migration. However, when the migration is run, **these statements have no effect**. The migration will run to completion, but the enum values in the database will remain the same. Therefore if you change the values of a enum, you should always manually create a migration using the `create_enum_migration` script.

##### Adding a value to an enum

1. Run `readonly-prod-psql` and get the current set of values for the enum using `SELECT enum_range(NULL::<enum_name>);`.

2. Run `recidiviz/tools/create_enum_migration.py` according to the instructions in its file docstring.

3. Update the generated migration according to the included comments using the enum values from the query.

##### Adding or dropping a column or table

1. Update `schema.py`.

2. Run `generate-auto-migration -m <migration name>`. (Note: This does not detect changes to enum values, which is why enum value migrations require the separate process outlined above.)

3. Check that the generated migration looks right.

##### Changing the type or format of existing data

1. Run `generate-empty-migration -m <migration name>`.

2. Follow the example in [split\_residence\_field](https://github.com/Recidiviz/pulse-data/blob/master/recidiviz/persistence/database/migrations/versions/2019_03_06_1027_0b32488901d4_split_residence_field.py) for how to apply a transformation to existing data.

#### Applying the migration

The migration and release process are tightly coupled. Once `schema.py` is updated, no new releases can be made until the database is migrated, and once the database is migrated, any new jobs on the current release (including both ingest jobs and BigQuery export jobs) will fail until a new release is made. Because of this, **do not merge a change to `schema.py` unless you can run the migration and deploy a new release before the next batch of jobs runs**.

1. Send a PR containing the migration for review.

2. Incorporate any review changes. Do not merge the PR yet.

3. Check the value in the `alembic_version` table in both dev and prod and ensure it's the same in both. If it isn't, check "Troubleshooting Alembic version issues" below.

4. Apply the migration to dev by running `migrate-dev-to-head`. Run `dev-psql` to verify that the outcome of the migration was successful. (If the migration failed but still completed (i.e. the change was committed but was incorrect, rather than failing and rolling back the transaction), **do not** try to manually undo the change. Just restore `dev-data` from its most recent daily backup, fix the migration script, and try again.)

5. Merge the PR. As mentioned above, once you perform this step, you **must** run the migration and deploy a new release before the next batch of jobs runs, or all jobs will fail.

6. Apply the migration to prod by running `migrate-prod-to-head`. Run `readonly-prod-psql` to verify that the outcome of the migration was successful.

#### Migration troubleshooting

##### Re-using existing enums for new columns

If you generate a migration that adds a new column that should use an existing enum type, Alembic by default will attempt to re-create that existing type, which will cause an error during migration.

To avoid this, you need to update the migration to ensure Alembic knows to re-use the existing type:

Import the `postgresql` dialect, since the migration must reference a PostgreSQL enum type.

```python
from sqlalchemy.dialects import postgresql
```

Then in the `sa.Column` constructor for the relevant column, change `sa.Enum` to `postgresql.ENUM` and add `create_type=False` to the enum constructor, e.g.:

```python
sa.Column(
    'some_column',
    postgresql.ENUM('VALUE_A', 'VALUE_B', create_type=False, name='enum_name'))
```

##### New table already present in database

When `Base.metadata.create_all` is called in `server.py` when a new job starts, `CREATE` statements will be executed for any tables found in `schema.py` that are not present in the database. This can create problems, because the newly created tables will be owned by the role used by the job to make writes, rather than the owner role.

The best way to avoid this problem is to follow the procedure outlined above and not deploy against staging or prod with a change in `schema.py` without first running the corresponding migration. However, if it does happen, follow the instructions in "Manual intervention" below (although you may possibly need to log in as a different role, depending on which role was used to create the table) and manually drop any unintentionally created tables, then follow the rest of the migration procedure to re-create them normally.

##### Alembic version issues

Alembic automatically manages a table called `alembic_version`. This table contains a single row containing the hash ID of the most recent migration run against the database. When you attempt to autogenerate or run a migration, if alembic does not see a migration file corresponding to this hash in the `versions` folder, the attempted action will fail.

Issues will arise if either your local copy of the `versions` folder does not match the `alembic_version` hash in the database, or if the `alembic_version` hashes in dev and prod don't match each other.

If your local copy of `versions` doesn't match the database, this is likely because someone else has run and merged a migration in the interim. In this case, move your migration file out of `versions`, merge the new migration from HEAD, use `generate-empty-migration -m <migration name>` to create a new empty migration file (to ensure it follows the most recent migration in sequence), and copy the changes from your old copy of the file to the new one.

If the `alembic_version` values in `dev-data` and `prod-data` don't match each other, there could be a number of reasons:

- A local manual migration was run against dev without being run against prod, presumably for testing purposes (this is fine). In this case, bring dev back into sync with prod via the steps in "Syncing dev via manual local migration" below.

- Someone ran the above workflow but didn't merge their PR before running against prod (this is mostly fine). In this case, just have them merge the PR, then pull the latest migration file locally.

- A checked-in migration was run against prod without being run against dev first (this is bad). In this case, do a manual check of prod via `readonly-prod-psql` to see if any issues were introduced by the migration. If not, bring dev up to date with prod via the steps in "Syncing dev via manual local migration" below.

- A local manual migration was run against prod without being checked in (this is very bad). Fixing this may require separately syncing both dev and prod with `schema.py` and then overwriting their `alembic_version` values, depending on what changes were made by the migration.

##### Syncing dev via manual local migration

This process can be used to get `dev-data` back into sync with `prod-data` and the `versions` folder if it gets out of sync.

1. Manually overwrite the `alembic_version` value in `dev-data` to match `prod-data` and the `versions` folder.

2. Autogenerate a migration using dev (rather than prod) as the reference with `set-alembic-dev-env && alembic -c recidiviz/persistence/database/alembic.ini revision --autogenerate -m <migration name>`.

3. Run `migrate-dev-to-head` to apply the local migration.

4. Delete the migration file.

5. Manually overwrite the `alembic_version` value again, to undo the hash update caused by running the local migration.

## Restoring backups

There are two types of database backup.

**Automatic backups** are created by Cloud SQL. A new automatic backup is created daily, with a TTL of 7 days, so one automatic backup is available for each day for the last 7 days. These backups are available for both `dev-data` and `prod-data`.

**Long-term backups** are created by a cron task in `cron.yaml` calling an endpoint in `backup_manager`. A new long-term backup is created weekly, with a TTL of 26 weeks, so one automatic backup is available for each week of the last 26 weeks. (Note Cloud SQL does not allow setting an actual TTL on a backup, so this "TTL" is manually enforced by the same cron task that creates new long-term backups.) These backups are only available for `prod-data`.

### Full restore

To restore the database to an earlier state in the case of a major error, visit the "Backups" tab for the `prod-data` instance on GCP, select a backup, and select "Restore". This will revert the database to its state at the time of that backup.

### Partial restore

> Lasciate ogni speranza, voi ch'entrate

In some cases, such as a scraper writing bad data to a single region without being detected for a long period of time, you may want to restore only some subset of rows, columns, or tables, rather than reverting the entire database to some earlier state.

This process is fairly finicky, especially for more complex restores. You should probably do a practice run against `dev-data` before trying to do this on prod.

1. On the "Backups" tab, make a manual backup of the current state of `prod-data`, to ensure the initial state can be restored if the process goes awry.

2. On GCP, create a new Cloud SQL instance with Postgres.

3. On the "Connections" tab of the new Cloud SQL instance, add `prod-data-client` as an authorized network.

4. On the "Backups" tab of `prod-data`, choose the appropriate backup and select "Restore". Update the target instance to the temporary Cloud SQL instance (**NOT** the regular `prod-data` instance), and execute the restore.

5. On `prod-data-client`, connect to the temporary instance with: `psql "sslmode=disable hostaddr=<IP of temporary Cloud SQL instance> user=postgres dbname=postgres"`

6. Export the data to be restored with: `\copy (<query for desired data>) TO '<filename>'`

7. Using the command in the "Manual intervention" section below, connect to `prod-data` with manual intervention permissions.

8. Create a temporary empty copy of the table you are restoring to: `CREATE TEMP TABLE <temp table name> AS SELECT <whichever columns you need> FROM <original table> LIMIT 0;`

9. Copy from the file you created previously to the temporary table: `\copy <temp table name> FROM '<filename>'`

10. Perform whatever standard SQL operations are required to copy the desired rows/columns from the temporary table to the regular table.

11. Repeat steps 5-10 for any other tables that need to be restored.

12. Delete the temporary Cloud SQL instance.

13. Delete the manual backup of `prod-data`.

## Manual intervention

In the (extremely rare) case where manual operations need to be performed directly on `prod-data` (which you shouldn't do), you can run the below command (but please don't) on `prod-data-client` to access the database with full permissions:

```bash
psql "sslmode=verify-ca sslrootcert=$SERVER_CA_PATH sslcert=$CLIENT_CERT_PATH \
sslkey=$CLIENT_KEY_PATH hostaddr=$PROD_DATA_IP user=$PROD_DATA_MIGRATION_USER \
dbname=$PROD_DATA_DB_NAME"
```

The `dev-psql` command always gives full permissions on `dev-data`, so manual operations there can be done freely.

## Setting up the VM

If a new `prod-data-client` VM needs to be created, follow the below process:

Create a Compute Engine VM in GCP running the latest version of Ubuntu. (Wait some time after creation before trying to SSH in; various setup processes will still hold needed locks.)

SSH in using `gcloud compute ssh <VM name>`

Run the following commands:

```bash
# Add repository for python3.7, which is currently not provided by Ubuntu
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt upgrade
# Should include all requirements installed by the project Dockerfile, plus
# postgresql-client
sudo apt install locales git libxml2-dev libxslt1-dev python3.7-dev \
    python3-pip default-jre postgresql-client
sudo pip3 install pipenv
```

Through the GCP console for the `prod-data` database, create a set of SSL certs for the VM to use. Whitelist the IP of the VM to connect for both `dev-data` and `prod-data`.

Save `server-ca.pem` and `client-cert.pem` in `/etc/ssl/certs`, and save `client-key.pem` in `/etc/ssl/private`.

Create `/usr/local/sbin/initial-setup` with the below content, and `chmod` it to be executable.

```bash
# Clone the repo and set up pipenv
git clone git@github.com:Recidiviz/pulse-data.git && \
cd pulse-data && pipenv sync --dev

# Make local copies of certs owned by user (to avoid issues with needing
# sudo to access shared certs)
mkdir ~/prod_data_certs/ && cp /etc/ssl/certs/server-ca.pem \
~/prod_data_certs/server-ca.pem && cp /etc/ssl/certs/client-cert.pem \
~/prod_data_certs/client-cert.pem && sudo cp /etc/ssl/private/client-key.pem \
~/prod_data_certs/client-key.pem && sudo chmod 0600 \
~/prod_data_certs/client-key.pem && sudo chown $USER: \
~/prod_data_certs/client-key.pem
```

Create `/etc/profile.d/.env` with the below content:

```bash
# These should match the ENV values set in the Dockerfile
export LC_ALL=en_US.UTF-8
export LC_CTYPE=en_US.UTF-8
export LANG=en_US.UTF-8
export TZ=America/New_York

export SERVER_CA_PATH=~/prod_data_certs/server-ca.pem
export CLIENT_CERT_PATH=~/prod_data_certs/client-cert.pem
export CLIENT_KEY_PATH=~/prod_data_certs/client-key.pem

export DEV_DATA_IP=<dev-data IP>
export DEV_DATA_PASSWORD=<dev-data password>
export DEV_DATA_USER=<dev-data username>
export DEV_DATA_DB_NAME=<dev-data database name>

export PROD_DATA_IP=<prod-data IP>
export PROD_DATA_PASSWORD=<prod-data password>
export PROD_DATA_READONLY_USER=<prod-data readonly username>
export PROD_DATA_MIGRATION_USER=<prod-data migration username>
export PROD_DATA_DB_NAME=<prod-data database name>
```

Create `/etc/profile.d/prod-data-aliases.sh` with the below content:

```bash
source /etc/profile.d/.env

alias dev-psql="psql \"sslmode=disable hostaddr=$DEV_DATA_IP \
user=$DEV_DATA_USER dbname=$DEV_DATA_DB_NAME\""

alias readonly-prod-psql="psql \"sslmode=verify-ca \
sslrootcert=$SERVER_CA_PATH sslcert=$CLIENT_CERT_PATH \
sslkey=$CLIENT_KEY_PATH hostaddr=$PROD_DATA_IP \
user=$PROD_DATA_READONLY_USER dbname=$PROD_DATA_DB_NAME\""

alias set-alembic-dev-env="export SQLALCHEMY_DB_NAME=$DEV_DATA_DB_NAME \
&& export SQLALCHEMY_DB_HOST=$DEV_DATA_IP \
&& export SQLALCHEMY_DB_PASSWORD=$DEV_DATA_PASSWORD \
&& export SQLALCHEMY_DB_USER=$DEV_DATA_USER \
&& export SQLALCHEMY_USE_SSL=0"

alias set-alembic-prod-env="export SQLALCHEMY_DB_NAME=$PROD_DATA_DB_NAME \
&& export SQLALCHEMY_DB_HOST=$PROD_DATA_IP \
&& export SQLALCHEMY_DB_PASSWORD=$PROD_DATA_PASSWORD \
&& export SQLALCHEMY_DB_USER=$PROD_DATA_MIGRATION_USER \
&& export SQLALCHEMY_USE_SSL=1 \
&& export SQLALCHEMY_SSL_CERT_PATH=$CLIENT_CERT_PATH \
&& export SQLALCHEMY_SSL_KEY_PATH=$CLIENT_KEY_PATH"

alias generate-empty-migration="alembic -c \
recidiviz/persistence/database/alembic.ini revision"

alias generate-auto-migration="set-alembic-prod-env && alembic -c \
recidiviz/persistence/database/alembic.ini revision --autogenerate"

alias migrate-dev-to-head="set-alembic-dev-env && alembic -c \
recidiviz/persistence/database/alembic.ini upgrade head"

function migrate-prod-to-head {
  read -p "Are you sure? (y to continue, any other key to exit): " response
  if [[ $response == "y" ]]
  then
    set-alembic-prod-env && alembic -c \
        recidiviz/persistence/database/alembic.ini upgrade head
  else
    echo "Cancelled"
  fi
}
```

At the end of `/etc/bash.bashrc`, add the below line. This makes aliases available in both interactive and login shells.

```bash
source /etc/profile.d/prod-data-aliases.sh
```

Set up a shared Git user for pushes from the VM by creating `/etc/gitconfig` if it doesn't exist, and then running:

```bash
sudo git config --system user.name <shared user name>
sudo git config --system user.email <shared user email>
```

In `/etc/ssh/ssh_config`, create a new host configuration for Github with the below content:

```bash
Host github.com
  HostName github.com
  IdentityFile /etc/ssh/github_key
  User git
```

Generate the SSH key pair with the below command. Do not set a passphrase, because access to the VM is already controlled by GCP auth.

```bash
ssh-keygen -t rsa -b 4096 -C "<shared user email>"
```

Move the created key pair to `/etc/ssh/github_key` and `/etc/ssh/github_key.pub`, then ensure they are accessible for all users by running:

```bash
sudo chmod 644 /etc/ssh/github_key
sudo chmod 644 /etc/ssh/github_key.pub
```

Add the public key to the shared Github account through the Github UI, and then run `ssh -T git@github.com` to confirm that the keys are properly set up.
