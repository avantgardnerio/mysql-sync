require('dotenv').config()
const fs = require('fs');
const mysql = require(`mysql-await`);
const yargs = require('yargs');
const _ = require('lodash');

const batchSize = 500;
const queryFks = fs.readFileSync("db/queries/get_fks.sql", "utf-8");
const queryCols = fs.readFileSync("db/queries/get_columns.sql", "utf-8");

const argv = yargs
    .option('dst-host', {description: 'Destination host', type: 'string'})
    .option('dst-port', {description: 'Destination port', type: 'string'})
    .option('dst-db', {description: 'Destination db name', type: 'string'})
    .option('dst-user', {description: 'Destination user', type: 'string'})
    .option('dst-pass', {description: 'Destination password', type: 'string'})
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

const getRowHashes = async (table, con) => {
    const md5s = Object.keys(table.cols).map(name => `md5(IFNULL(\`${name}\`, ''))`)
    const pkNames = getPk(table).map(col => `\`${col.COLUMN_NAME}\``);
    const sql = `select
                ${pkNames.join(', ')},
                md5(concat(${md5s.join(', ')})) as hash 
            from \`${table.name}\` 
            order by hash;`;
    const hashes = await con.awaitQuery(sql);
    return hashes;
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

const selectRows = async (selectSql, queryVals, pkExpr, srcCon, srcTable) => {
    if(queryVals.length === 0) return [];
    const sql = `${selectSql} (${queryVals.map(() => pkExpr).join(' or ')})`;
    const flat = queryVals.reduce((acc, cur) => [...acc, ...cur], []);
    const srcRows = (await srcCon.awaitQuery(sql, flat));
    const insertVals = srcRows.map(row => Object.values(row));
    const keys = Object.keys(srcTable.cols);
    for(let idx = 0; idx < keys.length; idx++) {
        const key = keys[idx];
        const col = srcTable.cols[key];
        if(col.COLUMN_TYPE !== 'date') continue;
        for(let row of insertVals) {
            const val = row[idx];
            if(val === '0000-00-00') {
                console.log("Fixed null date");
                row[idx] = null;
            }
        }
    }
    queryVals.splice(0, queryVals.length);
    return insertVals;
};

const syncBatch = async (deleteVals, queryVals, dstCon, deleteSql, tableName, pkExpr, selectSql, srcCon, insertSql, srcTable) => {
    deleteVals.push(...queryVals);
    await deleteRows(dstCon, deleteSql, deleteVals, tableName, pkExpr);
    const insertVals = await selectRows(selectSql, queryVals, pkExpr, srcCon, srcTable);
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

        const tableNames = _.intersection(Object.keys(srcTables), Object.keys(dstTables));
        for (const tableName of tableNames) {
            console.log(`Syncing ${tableName}...`)
            const srcTable = srcTables[tableName];
            const dstTable = dstTables[tableName];
            if (Object.values(srcTable.cols).filter(col => col.DATA_TYPE === 'geometry').length > 0) {
                console.warn(`Skipping table, type not implemented: geometry`);
                continue;
            }
            const srcHashes = await getRowHashes(srcTable, srcCon);
            const dstHashes = await getRowHashes(dstTable, dstCon);

            // Build queries
            const pkExpr = `(${getPk(srcTable).map(col => `\`${col.COLUMN_NAME}\`=?`).join(' and ')})`;
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
                    const pkVals = getPk(srcTable).map(col => srcHashRow[col.COLUMN_NAME]);
                    deleteVals.push(pkVals);
                    dstIdx++;
                } else if (srcHash < dstHash) { // abc -> ac = insert b
                    const pkVals = getPk(srcTable).map(col => srcHashRow[col.COLUMN_NAME]);
                    queryVals.push(pkVals);
                    srcIdx++;
                } else { // abc -> abc = no-op
                    srcIdx++;
                    dstIdx++;
                }

                // execute a query batch
                if (deleteVals.length + queryVals.length === batchSize) {
                    await syncBatch(deleteVals, queryVals, dstCon, deleteSql, tableName, pkExpr, selectSql, srcCon, insertSql, srcTable);
                }
            }
            await syncBatch(deleteVals, queryVals, dstCon, deleteSql, tableName, pkExpr, selectSql, srcCon, insertSql, srcTable);
            console.log(`Finished ${tableName}`)
        }
    } finally {
        await dstCon.awaitQuery(`SET FOREIGN_KEY_CHECKS=1;`);
    }

    await srcCon.awaitEnd();
})().catch((ex) => {
    throw ex;
});
