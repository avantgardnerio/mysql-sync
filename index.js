require('dotenv').config()
const fs = require('fs');
const mysql = require(`mysql-await`);
const yargs = require('yargs');
const _ = require('lodash');
const cliProgress = require('cli-progress');

// https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet 67MB
// https://github.com/mysqljs/mysql/issues/1698 4MB
const maxAllowedPacket = 4 * 1024 * 1024;
const fudgeFactor = 1;
const batchSize = 500;
const limit = 10 * batchSize;
const queryFks = fs.readFileSync("db/queries/get_fks.sql", "utf-8");
const queryCols = fs.readFileSync("db/queries/get_columns.sql", "utf-8");

const argv = yargs
    .option('dst-host', {description: 'Destination host', type: 'string'})
    .option('dst-port', {description: 'Destination port', type: 'string'})
    .option('dst-db', {description: 'Destination db name', type: 'string'})
    .option('dst-user', {description: 'Destination user', type: 'string'})
    .option('dst-pass', {description: 'Destination password', type: 'string'})
    .option('seed-table', {description: 'Name of root table to sync', type: 'string'})
    .option('pk', {description: 'Value of primary key to start sync', type: 'array'})
    .help().alias('help', 'h')
    .argv;

const getRels = async (con, db) => {
    const fks = await con.awaitQuery(queryFks, db);
    return fks;
};

const getPk = (table) => Object.values(table.cols).filter(col => col.COLUMN_KEY === 'PRI');

const getTables = async (con, db) => {
    const cols = await con.awaitQuery(queryCols, db);
    const tables = cols.reduce((acc, cur) => {
        acc[cur.TABLE_NAME] = acc[cur.TABLE_NAME] || {"name": cur.TABLE_NAME, "cols": {}};
        acc[cur.TABLE_NAME].cols[cur.COLUMN_NAME] = cur;
        return acc;
    }, {});
    return tables;
};

const srcConfig = {
    "connectionLimit": 10,
    "host": process.env.DATABASE_PARAMS_HOST,
    "port": process.env.DATABASE_PARAMS_PORT,
    "database": process.env.DATABASE_PARAMS_DBNAME,
    "user": process.env.DATABASE_PARAMS_USERNAME,
    "password": process.env.DATABASE_PARAMS_PASSWORD,
};
const dstConfig = {
    "connectionLimit": 10,
    "host": argv['dst-host'] || '127.0.0.1',
    "port": argv['dst-port'] || '3306',
    "database": argv['dst-db'],
    "user": argv['dst-user'] || 'root',
    "password": argv['dst-pass'] || '',
};
const srcCon = mysql.createConnection(srcConfig);
srcCon.on(`error`, (err) => console.error(`Connection error ${err.code}`));
const dstCon = mysql.createConnection(dstConfig);
dstCon.on(`error`, (err) => console.error(`Connection error ${err.code}`));
console.log(`Syncing ${srcConfig.host}:${srcConfig.port} -> ${dstConfig.host}:${dstConfig.port}`);
(async () => {
    await dstCon.awaitQuery(`SET FOREIGN_KEY_CHECKS=0;`);
    await dstCon.awaitQuery(`SET UNIQUE_CHECKS=0;`);
    try {
        const srcTables = await getTables(srcCon, srcConfig.database);
        const dstTables = await getTables(dstCon, dstConfig.database);

        const tableName = argv['seed-table'];
        const srcTable = srcTables[tableName];
        const dstTable = dstTables[tableName];

        // Build queries
        const srcCols = Object.values(srcTable.cols).map(col => `\`${col.COLUMN_NAME}\``);
        const dstCols = Object.values(dstTable.cols).map(col => `\`${col.COLUMN_NAME}\``);
        const colNames = _.intersection(srcCols, dstCols);
        let pkCols = getPk(dstTable);
        if(pkCols.length === 0) {
            pkCols = getPk(srcTable);
        }
        const pkExpr = `(${pkCols.map(col => `\`${col.COLUMN_NAME}\`=?`).join(' and ')})`;
        const selectSql = `select ${colNames.join(', ')} from \`${tableName}\` where ${pkExpr}`;
        const pkVals = argv['pk'];
        const seedRow = (await srcCon.awaitQuery(selectSql, pkVals))[0];

        console.log('Syncing...')
    } finally {
        try {
            console.log(`Enabling FK checks...`);
            await dstCon.awaitQuery(`SET FOREIGN_KEY_CHECKS=1;`);
            console.log(`Enabling unique checks...`);
            await dstCon.awaitQuery(`SET UNIQUE_CHECKS=1;`);
        } catch (ex) {
            console.warn("Unable to re-enable constraints. Try running it again.")
        }
    }

    console.log(`Closing connections...`);
    await srcCon.awaitEnd();
    await dstCon.awaitEnd();
    console.log(`Done!`);
})().catch((ex) => {
    console.error("Error synchronizing databases!", ex);
    srcCon
        .awaitEnd()
        .then(() => dstCon.awaitEnd())
        .finally(() => {
            process.exit(1);
        });
});
