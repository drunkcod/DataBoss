# DataBoss

DataBoss helps manage database changes to aid developers and DBA's wanting to quickly
evolve or refactor their database designs.

It does this by keeping track of change scripts to be applied across instances.

## Installation

Install DataBoss as a dotnet tool.

```
dotnet tool install databoss -g
```

## Getting Started

When initializing DataBoss, if the database you want to manage doesn't exist
DataBoss creates it for you. Otherwise initialization only creates data tables
for migration history.

1. In your working directory, create a `MyTestDb.databoss` [target file](#target-files)
1. Initialize DataBoss and start version history by running `databoss init -Target MyTestDb.databoss`
1. Create numbered migration .sql files in your migrations folder, e.g. `./migrations/01 Create MyTable.sql`
1. To compare migration history with pending migrations, run `databoss status -Target MyTestDb.databoss`
1. Run migrations by executing `databoss update -Target MyTestDb.databoss`

## Target Files

.databoss target files specifies which database your running migrations for.

```xml
<db database="MyTestDb" server=".">
    <migrations path="./migrations/" />
</db>
```

## Change Log

**0.0.0.7**
* Failing migrations reported via negative status code.
* Commands made public to ease library use.

**0.0.0.6**
* Exit code tries to be helpful, "status" returns number of pending migrations.
