# DataBoss

DataBoss helps manage database changes to aid developers and DBA's wanting to quickly
evolve or refactor their database designs.

It does this by keeping track of change scripts to be applied across instances.

## Change Log

**0.0.0.7**
* Failing migrations reported via negative status code.
* Commands made public to ease library use.

**0.0.0.6**
* Exit code tries to be helpful, "status" returns number of pending migrations.
