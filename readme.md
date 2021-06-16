# mysql-sync

A utility for synchronizing two running MySQL instances. Be aware data may be lost on the target if it is not in the source!

## developer setup
1. git clone
1. npm install
1. npm run

## Audit tool

`node --max_old_space_size=4096 audit.js`

Make sure you have .env set up with database parameters:

```
DATABASE_PARAMS_HOST=<host>
DATABASE_PARAMS_PORT=<port>
DATABASE_PARAMS_DBNAME=<dbname>
DATABASE_PARAMS_USERNAME=<username>
DATABASE_PARAMS_PASSWORD=<password>
```

This will create a folder `<host>.<dbname>` with the following:

```
tables.csv
<table-1>.csv.gz
<table-2>.csv.gz
...
<table-n>.csv.gz
```

tables.csv contains name, counts and full checksum of tables. The individual table files contains a checksum for each row.

## Compare Tool

After running the audit tool on two DBs, run the compare tool:

```
node --max_old_space_size=4096 ./compare-audits.js .env-a .env-b
```

where the env file match the one for the audit tool.

This will go through the generated csv files, and print the differences between them. In the case of checksum failures on individual rows, it will query for the failed rows and display which columns are different.

