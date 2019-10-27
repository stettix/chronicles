## Installation

TBD

## Updating Spark jobs to write versioned data

TBD

## Using the CLI

Chronicles comes with a command line interface (CLI) that can be used to query version history, as well as performing actions such as adding and removing partitions, or changing which version of a table that is 'current'.

### Configuring the CLI

The CLI needs to know how to connect to the store for version information, so the configuration details for this needs to be provided.
The configuration is picked up from a file `~/.chronicles/config`.
Currently, the CLI only supports direct connection to the version repository database (connection via an API service may be provided in the future).
The configuration file for such a connection uses the [HOCON](https://github.com/lightbend/config/blob/master/HOCON.md) format, and looks like the following example:

```hocon
type: "db-config"
db-type: postgresql
hostname: "<host name for the database instance>"
port: 5432 # Or change for non-default port numbers
db-name: "chronicles" # Can be any database that exists on the given Postgresql instance.
username: "<user name for CLI user>"
password: ""
```

### Overview of commands

To show all available commands, run:

```bash
$ chronicles --help
```

This will show a summary of available commands, e.g.:

```
Options and flags:
    --help
        Display this help text.

Subcommands:
    tables
        List details about tables
    init
        Initialise version tracking for table
    partitions
        List partitions for table
    log
        List version history for table
    partition
        Modify table partition
[...]
```

The following describes individual commands in more detail.

### `tables` command

The `tables` command lists all tables that Chronicles is aware of. For example:

```bash
$ chronicles tables
```

May produce the example output:

```
user_logins
user_clicks
purchases
```

### `init` command

The `init` command is used to initialise a table in the Chronicles version repository.
This basically lets Chronicles know of the existence of a table, and provides some metadata about the table.

The format of the command is:

```bash
$ chronicles init [--isSnapshot] --message <string> <table name>
```

The `--isSnapshot` flag is used to inform Chronicles that a table is unpartitioned, i.e. a "snapshot" table.
By default, tables are defined as partitioned.

The `--message` option provides an informational message that will be available in the version history of the table.

### `log` command

The `log` command shows the change history of a table.
The format of the command is:

```bash
$ chronicles log <table name>
```

**Further commands TBD!**
