require('dotenv').config()
const fs = require('fs').promises;
const mysql = require(`mysql-await`);
const yargs = require('yargs');

const argv = yargs
    .option('schema', {
        alias: 's',
        description: 'Schema to sync',
        type: 'string',
    })
    .help().alias('help', 'h')
    .argv;

(async () => {
    const config = {
        "connectionLimit": 10,
        "host": process.env.DATABASE_PARAMS_HOST,
        "port": process.env.DATABASE_PARAMS_PORT,
        "database": process.env.DATABASE_PARAMS_DBNAME,
        "user": process.env.DATABASE_PARAMS_USERNAME,
        "password": process.env.DATABASE_PARAMS_PASSWORD,
    };
    const con = mysql.createConnection(config);
    con.on(`error`, (err) => console.error(`Connection error ${err.code}`));

    const fks = await con.awaitQuery(await fs.readFile("db/queries/get_fks.sql", "utf-8"), config.database);
    const cols = await con.awaitQuery(await fs.readFile("db/queries/get_columns.sql", "utf-8"), config.database);
    const tables = cols.reduce((acc, cur) => {
        acc[cur.TABLE_NAME] = acc[cur.TABLE_NAME] || {"name": cur.TABLE_NAME, "cols": {}};
        acc[cur.TABLE_NAME].cols[cur.COLUMN_NAME] = cur;
        return acc;
    }, {});

    for(const table of Object.values(tables)) {
        const md5s = Object.keys(table.cols).map(name => `md5(IFNULL(\`${name}\`, ''))`)
        let sql = `select 
                md5(concat(${md5s.join(', ')})) as hash 
            from \`${table.name}\` 
            order by hash;`;

        const srcHashes = await con.awaitQuery(sql);
        console.log(srcHashes)
    }

    con.awaitEnd();
})();