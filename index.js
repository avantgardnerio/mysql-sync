require('dotenv').config()
const fs = require('fs');
const mysql = require(`mysql-await`);
const yargs = require('yargs');
const _ = require('lodash');
const cliProgress = require('cli-progress');

// TODO: ensure batchSize results in less than max packet size https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet
const batchSize = 300; // max_allowed_packet=67108864
const limit = 10 * batchSize;
const queryFks = fs.readFileSync("db/queries/get_fks.sql", "utf-8");
const queryCols = fs.readFileSync("db/queries/get_columns.sql", "utf-8");

const argv = yargs
    .option('dst-host', {description: 'Destination host', type: 'string'})
    .option('dst-port', {description: 'Destination port', type: 'string'})
    .option('dst-db', {description: 'Destination db name', type: 'string'})
    .option('dst-user', {description: 'Destination user', type: 'string'})
    .option('dst-pass', {description: 'Destination password', type: 'string'})
    .option('first-table', {description: 'Name of first table to sync', type: 'string'})
    .help().alias('help', 'h')
    .argv;

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

const row2hash = (row) => {
    return {
        pk: Object.keys(row).filter(colName => colName !== 'hash').map(colName => row[colName]).reduce((acc, cur) => [...acc, cur], []),
        hash: row.hash
    }
}

const getSrcHashes = async (table, con, pkCols, page = 0) => {
    const offset = page * limit;
    const md5s = Object.keys(table.cols).map(name => `md5(IFNULL(\`${name}\`, ''))`).join(', ');
    const pkNames = pkCols.map(col => `\`${col.COLUMN_NAME}\``).join(', ');
    const sql = `select ${pkNames}, md5(concat(${md5s})) as hash from \`${table.name}\` order by ${pkNames} limit ${limit} offset ${offset}`;
    const hashes = await con.awaitQuery(sql);
    const rows = hashes.map(row => row2hash(row));
    return rows;
};

const getDstHashes = async (table, con, pkCols, min, max) => {
    const md5s = Object.keys(table.cols).map(name => `md5(IFNULL(\`${name}\`, ''))`).join(', ');
    const pkNames = pkCols.map(col => `\`${col.COLUMN_NAME}\``).join(', ');
    const minClause = pkCols.map(col => `\`${col.COLUMN_NAME}\` >= ?`).join(' and ');
    const maxClause = pkCols.map(col => `\`${col.COLUMN_NAME}\` <= ?`).join(' and ');
    const sql = `select ${pkNames}, md5(concat(${md5s})) as hash from \`${table.name}\` where ${minClause} and ${maxClause} order by ${pkNames}`;
    const params = [...min, ...max];
    const hashes = await con.awaitQuery(sql, params);
    const rows = hashes.map(row => row2hash(row));
    return rows;
};

const getRowCount = async (tableName, con) => {
    const sql = `select count(*) as cnt from \`${tableName}\``;
    const res = (await con.awaitQuery(sql))[0].cnt;
    return res;
};

const insertRows = async (dstCon, insertSql, values, tableName, pkExpr) => {
    if(values.length === 0) return;
    const sql = `${insertSql} ${values.map(row => `(${row.map(() => `?`).join(', ')})`).join(', ')}`;
    const flat = values.reduce((acc, cur) => [...acc, ...cur], []);
    // try {
        const res = await dstCon.awaitQuery(sql, flat);
        if (res.affectedRows !== values.length) {
            console.warn(`Problem inserting into ${tableName} ${pkExpr} affected ${res.affectedRows} rows ${res.message}`);
        }
    // } catch (ex) {
    //     throw ex;
    // }
    values.splice(0, values.length);
}

const deleteRows = async (dstCon, deleteSql, values, tableName, pkExpr) => {
    if(values.length === 0) return;
    const sql = `${deleteSql} (${values.map(() => pkExpr).join(' or ')})`;
    const flat = values.reduce((acc, cur) => [...acc, ...cur], []);
    const res = await dstCon.awaitQuery(sql, flat);
    values.splice(0, values.length);
};

const selectRows = async (selectSql, queryVals, pkExpr, srcCon, dstTable) => {
    if(queryVals.length === 0) return [];
    const sql = `${selectSql} (${queryVals.map(() => pkExpr).join(' or ')})`;
    const flat = queryVals.reduce((acc, cur) => [...acc, ...cur], []);
    const srcRows = (await srcCon.awaitQuery(sql, flat));
    const insertVals = srcRows.map(row => Object.values(row));
    const keys = Object.keys(dstTable.cols);
    for(let idx = 0; idx < keys.length; idx++) {
        const key = keys[idx];
        const col = dstTable.cols[key];
        if(col.COLUMN_TYPE === 'date') {
            for(let row of insertVals) {
                const val = row[idx];
                if(val === '0000-00-00') {
                    row[idx] = null;
                }
            }
        }
        if(col.COLUMN_TYPE === 'datetime') {
            for(let row of insertVals) {
                const val = row[idx];
                if(val === '0000-00-00 00:00:00') {
                    row[idx] = null;
                }
            }
        }
        if(col.COLUMN_TYPE === 'timestamp') {
            for(let row of insertVals) {
                const val = row[idx];
                if(val === '0000-00-00 00:00:00') {
                    row[idx] = null;
                }
            }
        }
        if(col.COLUMN_TYPE.startsWith('enum')) {
            for(let row of insertVals) {
                const val = row[idx];
                if(val === '') {
                    row[idx] = null;
                }
            }
        }
    }
    queryVals.splice(0, queryVals.length);
    return insertVals;
};

const syncBatch = async (deleteVals, queryVals, dstCon, deleteSql, tableName, pkExpr, selectSql, srcCon, insertSql, dstTable) => {
    deleteVals.push(...queryVals);
    await deleteRows(dstCon, deleteSql, deleteVals, tableName, pkExpr, dstTable);
    const insertVals = await selectRows(selectSql, queryVals, pkExpr, srcCon, dstTable);
    await insertRows(dstCon, insertSql, insertVals, tableName, pkExpr);
};

(async () => {
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
    await dstCon.awaitQuery(`SET FOREIGN_KEY_CHECKS=0;`);
    try {
        const srcTables = await getTables(srcCon, srcConfig.database);
        const dstTables = await getTables(dstCon, dstConfig.database);

        let running = argv['first-table'] === undefined;
        const tableNames = _.intersection(Object.keys(srcTables), Object.keys(dstTables));
        for (const tableName of tableNames) {
            if(tableName === argv['first-table']) { // for debugging
                running = true;
            }
            if(!running) continue;
            const rowCount = await getRowCount(tableName, srcCon);
            console.log(`Syncing ${rowCount} rows on table ${tableName}...`)
            const progress = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
            progress.start(rowCount, 0);

            const srcTable = srcTables[tableName];
            const dstTable = dstTables[tableName];
            if (Object.values(srcTable.cols).filter(col => col.DATA_TYPE === 'geometry').length > 0) {
                console.warn(`Skipping table, type not implemented: geometry`);
                continue;
            }
            let page = 0;
            const srcHashes = await getSrcHashes(srcTable, srcCon, getPk(dstTable), page);
            const min = srcHashes[0].pk;
            const max = srcHashes[srcHashes.length - 1].pk;
            const dstHashes = await getDstHashes(dstTable, dstCon, getPk(dstTable), min, max);

            // Build queries
            const pkExpr = `(${getPk(dstTable).map(col => `\`${col.COLUMN_NAME}\`=?`).join(' and ')})`;
            const colNames = Object.values(srcTable.cols).map(col => `\`${col.COLUMN_NAME}\``);
            const selectSql = `select ${colNames.join(', ')} from \`${tableName}\` where `;
            const insertSql = `insert into \`${tableName}\` (${colNames.join(', ')}) values `
            const deleteSql = `delete from \`${tableName}\` where `
            const queryVals = [];
            const deleteVals = [];

            let srcIdx = 0;
            let dstIdx = 0;
            while (srcIdx < srcHashes.length || dstIdx < dstHashes.length) {
                let srcHashRow = srcHashes[srcIdx];
                let dstHashRow = dstHashes[dstIdx];
                let srcHash = srcHashRow?.hash || "z";
                let dstHash = dstHashRow?.hash || "z";

                // cache values to insert or delete
                if (srcHash > dstHash) { // ac -> abc = delete b
                    const pkVals = getPk(dstTable).map(col => {
                        if(srcHashRow === undefined) {
                            console.log(`${srcHashRow} ${dstHashRow} ${srcHash} ${dstHash}`);
                        }
                        return srcHashRow[col.COLUMN_NAME];
                    });
                    deleteVals.push(pkVals);
                    dstIdx++;
                } else if (srcHash < dstHash) { // abc -> ac = insert b
                    const pkVals = getPk(dstTable).map(col => srcHashRow[col.COLUMN_NAME]);
                    queryVals.push(pkVals);
                    srcIdx++;
                } else { // abc -> abc = no-op
                    srcIdx++;
                    dstIdx++;
                }

                // execute a query batch
                if (deleteVals.length + queryVals.length === batchSize) {
                    await syncBatch(deleteVals, queryVals, dstCon, deleteSql, tableName, pkExpr, selectSql, srcCon, insertSql, dstTable);
                    progress.update(srcIdx);
                }
            }
            await syncBatch(deleteVals, queryVals, dstCon, deleteSql, tableName, pkExpr, selectSql, srcCon, insertSql, dstTable);
            progress.stop();
            console.log(`Finished ${tableName}`)
        }
    } finally {
        try {
            await dstCon.awaitQuery(`SET FOREIGN_KEY_CHECKS=1;`);
        } catch (ex) {
            console.warn("Unable to re-enable constraints. Try running it again.")
        }
    }

    await srcCon.awaitEnd();
    await dstCon.awaitEnd();
})().catch((ex) => {
    throw ex;
});
