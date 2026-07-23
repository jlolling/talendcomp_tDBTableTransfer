# Component to transfer tables or queries in a fast manner from one database to another.
The source database can by different from the target database.

Here the [Documentations for the MySQL edition](https://github.com/jlolling/talendcomp_tDBTableTransfer/blob/master/doc/tMysqlTableTransfer.pdf)

Here the [Documentations for the PostgreSQL edition](https://github.com/jlolling/talendcomp_tDBTableTransfer/blob/master/doc/tPostgresqlTableTransfer.pdf)

All other database types have less options and you can refer to the tPostgresql edition documentation. The option what should happens if keys are doubled does not exist here.

[Help page to download and install custom components](https://jan-lolling.de/) 

## Important note:

If you use SAP HANA as source you will probably experience the strange error message:

```SAP DBTech JDBC: SQL statement would generate a row count```

This is because a misinterpretation of block comments in the recent HANA versions.
To prevent this: Switch off the option "Add application name as comment to the statements" in the Advanced Settings of the component.
