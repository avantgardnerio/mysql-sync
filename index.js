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

    const sql = await fs.readFile("db/queries/get_fks.sql", "utf-8");

    const result = await con.awaitQuery(sql, argv.schema);
    console.log(result);

    con.awaitEnd();
})();