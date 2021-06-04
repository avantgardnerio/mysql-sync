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
    const rels = fks.reduce((acc, cur) => {
        let obj = acc[cur.CONSTRAINT_NAME] || {name: cur.CONSTRAINT_NAME, cols: []};
        obj.parent = cur.REFERENCED_TABLE_NAME;
        obj.child = cur.TABLE_NAME;
        obj.cols[cur.ORDINAL_POSITION - 1] = {
            parent: cur.REFERENCED_COLUMN_NAME,
            child: cur.COLUMN_NAME,
        };
        acc[cur.CONSTRAINT_NAME] = obj;
        return acc;
    }, {});
    return rels;
};

const indexRels = (rels, fieldName) => {
    const idx = Object.keys(rels).reduce((acc, cur) => {
        const rel = rels[cur];
        const key = rel[fieldName];
        acc[key] = acc[key] || [];
        acc[key].push(rel);
        return acc;
    }, {});
    return idx;
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
    queryVals = queryVals.slice(0, 500); // MySQL freaks out with large queries

    // run query
    const sql = `${selectSql} (${queryVals.map(() => pkExpr).join(' or ')})`;
    const flat = queryVals.reduce((acc, cur) => [...acc, ...cur], []);
    const srcRows = (await srcCon.awaitQuery(sql, flat));
    const insertVals = srcRows.map(row => Object.values(row));

    // limit rows to maxPacketSize
    let size = 0;
    for(let idx = 0; idx < insertVals.length; idx++) {
        let row = insertVals[idx];
        size += row.reduce((acc, cur) => {
            if(typeof cur === 'string' || cur instanceof String) {
                return acc + cur.length;
            } else if(typeof cur === 'number') {
                return acc + cur.toString().length;
            } else if(cur instanceof Buffer) {
                return acc + cur.length;
            } else if(cur instanceof Date) {
                return acc + cur.toString().length;
            } else if(cur === null) {
                return acc + 4;
            } else {
                throw new Error(`Unknown type: ${typeof cur}`);
            }
        }, 0);
        if(size * fudgeFactor > maxAllowedPacket) {
            insertVals.length = idx;
            break;
        }
    }

    // convert MariaDB nulls to MySQL nulls
    const keys = Object.keys(dstTable.cols);
    for(let idx = 0; idx < keys.length; idx++) {
        const key = keys[idx];
        const col = dstTable.cols[key];
        if(argv['zero-dates'] === undefined) {
            if(col.COLUMN_TYPE === 'date') {
                insertVals.filter(row => row[idx] === '0000-00-00').forEach(row => {
                    row[idx] = null
                });
            }
            if(col.COLUMN_TYPE === 'datetime') {
                insertVals.filter(row => row[idx] === '0000-00-00 00:00:00').forEach(row => {
                    row[idx] = null
                });
            }
            if(col.COLUMN_TYPE === 'timestamp') {
                insertVals.filter(row => row[idx] === '0000-00-00 00:00:00').forEach(row => {
                    row[idx] = null
                });
            }
        }
        if(col.COLUMN_TYPE.startsWith('enum')) {
            insertVals.filter(row => row[idx] === '').forEach(row => {
                row[idx] = null
            });
        }
    }

    // return the values to insert
    return insertVals;
};

const syncBatch = async (deleteVals, queryVals, dstCon, deleteSql, tableName, pkExpr, selectSql, srcCon, insertSql, dstTable) => {
    await deleteRows(dstCon, deleteSql, deleteVals, tableName, pkExpr, dstTable);
    while(queryVals.length > 0) {
        console.log(`${queryVals.length} remaining...`);
        const insertVals = await selectRows(selectSql, queryVals, pkExpr, srcCon, dstTable);
        deleteVals = queryVals.splice(0, insertVals.length);
        await deleteRows(dstCon, deleteSql, deleteVals, tableName, pkExpr, dstTable);
        await insertRows(dstCon, insertSql, insertVals, tableName, pkExpr);
    }
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

const syncRows = async (srcTables, tableName, dstTables, queryVals, parent2child, path = []) => {
    if(queryVals.length === 0) {
        return;
    }
    path = [...path, tableName];
    console.log(`Syncing ${path.join('->')}...`)
    const srcTable = srcTables[tableName];
    const dstTable = dstTables[tableName];

    // Build queries
    const srcCols = Object.values(srcTable.cols).map(col => `\`${col.COLUMN_NAME}\``);
    const dstCols = Object.values(dstTable.cols).map(col => `\`${col.COLUMN_NAME}\``);
    const colNames = _.intersection(srcCols, dstCols);
    let pkCols = getPk(dstTable);
    if (pkCols.length === 0) {
        pkCols = getPk(srcTable);
    }
    const pkExpr = `(${pkCols.map(col => `\`${col.COLUMN_NAME}\`=?`).join(' and ')})`;
    const selectSql = `select ${colNames.join(', ')} from \`${tableName}\` where `;
    const insertSql = `insert into \`${tableName}\` (${colNames.join(', ')}) values `;
    const deleteSql = `delete from \`${tableName}\` where `;

    const deleteVals = [];
    const parentVals = [...queryVals];
    await syncBatch(deleteVals, queryVals, dstCon, deleteSql, tableName, pkExpr, selectSql, srcCon, insertSql, dstTable);

    // recurse into children
    const children = parent2child[tableName] || [];
    for(let child of children) {
        const tableName = child.child;
        const fkCols = child.cols.map(col => col.child);
        const fkExpr = `(${fkCols.map(col => `\`${col}\`=?`).join(' and ')})`;
        const dstTable = dstTables[tableName];
        let pkCols = getPk(dstTable);
        if (pkCols.length === 0) {
            pkCols = getPk(srcTable);
        }
        const pkNames = pkCols.map(it => it.COLUMN_NAME);
        const selectSql = `select ${pkNames.join(', ')} from \`${tableName}\` where ${fkExpr}`;
        const flat = parentVals.reduce((acc, cur) => [...acc, ...cur], []);
        const childPkRows = (await srcCon.awaitQuery(selectSql, flat));
        const childVals = childPkRows.map(row => Object.values(row));

        await syncRows(srcTables, tableName, dstTables, childVals, parent2child, path);
    }
}

(async () => {
    await dstCon.awaitQuery(`SET FOREIGN_KEY_CHECKS=0;`);
    await dstCon.awaitQuery(`SET UNIQUE_CHECKS=0;`);
    try {
        const srcTables = await getTables(srcCon, srcConfig.database);
        const dstTables = await getTables(dstCon, dstConfig.database);
        const rels = await getRels(dstCon, dstConfig.database);
        const parent2child = indexRels(rels, 'parent');
        const child2parent = indexRels(rels, 'child');

        const tableName = argv['seed-table'];
        const pkVals = argv['pk'];
        const queryVals = [];
        queryVals.push(pkVals);
        await syncRows(srcTables, tableName, dstTables, queryVals, parent2child);
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
