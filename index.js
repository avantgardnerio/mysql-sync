require('dotenv').config()
const fs = require('fs');
const mysql = require(`mysql-await`);
const yargs = require('yargs');
const _ = require('lodash');

const query_fks = fs.readFileSync("db/queries/get_fks.sql", "utf-8");
const query_cols = fs.readFileSync("db/queries/get_columns.sql", "utf-8");

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
    const cols = await con.awaitQuery(query_cols, db);
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

    const srcTables = await getTables(srcCon, srcConfig.database);
    const dstTables = await getTables(dstCon, dstConfig.database);

    const tableNames = _.intersection(Object.keys(srcTables), Object.keys(dstTables));
    for(const tableName of tableNames) {
        console.log(`Syncing ${tableName}...`)
        const srcTable = srcTables[tableName];
        const dstTable = dstTables[tableName];
        const srcHashes = await getRowHashes(srcTable, srcCon);
        const dstHashes = await getRowHashes(dstTable, dstCon);
        let srcIdx = 0;
        let dstIdx = 0;
        while(srcIdx < srcHashes.length || dstIdx < dstHashes.length) {
            let srcHashRow = srcHashes[srcIdx];
            let dstHashRow = dstHashes[dstIdx];
            let srcHash = srcHashRow?.hash || "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
            let dstHash = dstHashRow?.hash || "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";

            if(srcHash > dstHash) { // ac -> abc = delete b
                // TODO: delete from dst
                dstIdx++;
            } else if(srcHash < dstHash) { // abc -> ac = insert b
                const pkVals = getPk(srcTable).map(col => srcHashRow[col.COLUMN_NAME]);
                const pkExpr = getPk(srcTable).map(col => `\`${col.COLUMN_NAME}\`=?`).join(' and ');
                const colNames = Object.values(srcTable.cols).map(col => `\`${col.COLUMN_NAME}\``);
                const params = colNames.map(() => '?');
                const select_sql = `select
                                        ${colNames.join(', ')}
                                    from \`${tableName}\`
                                    where ${pkExpr}`;
                const insert_sql = `insert into \`${tableName}\` (${colNames.join(', ')}) 
                                    values (${params.join(', ')})`;
                const srcRow = (await srcCon.awaitQuery(select_sql, pkVals))[0];
                const res = await dstCon.awaitQuery(insert_sql, Object.values(srcRow));
                if(res.affectedRows !== 1) {
                    console.warn(`Problem inserting into ${tableName} ${pkExpr} ${pkVals} affected ${res.affectedRows} rows ${res.message}`);
                }
                srcIdx++;
            } else { // abc -> abc = no-op
                srcIdx++;
                dstIdx++;
            }
        }
        console.log(`Finished ${tableName}`)
    }

    await srcCon.awaitEnd();
})();
