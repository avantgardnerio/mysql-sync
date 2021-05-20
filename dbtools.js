const fs = require('fs');
const queryFks = fs.readFileSync("db/queries/get_fks.sql", "utf-8");
const queryCols = fs.readFileSync("db/queries/get_columns.sql", "utf-8");
const mysql = require(`mysql-await`);
const getPk = (table) => Object.values(table.cols).filter(col => col.COLUMN_KEY === 'PRI');

const getTables = async (con, db = con.config.database) => {
    const cols = await con.awaitQuery(queryCols, db);
    const tables = cols.reduce((acc, cur) => {
        acc[cur.TABLE_NAME] = acc[cur.TABLE_NAME] || {"name": cur.TABLE_NAME, "cols": {}};
        acc[cur.TABLE_NAME].cols[cur.COLUMN_NAME] = cur;
        return acc;
    }, {});
    return tables;
};

function makeConfig(cfg = process.env) {
    return {
        "connectionLimit": 10,
        "host": cfg.DATABASE_PARAMS_HOST,
        "port": cfg.DATABASE_PARAMS_PORT,
        "database": cfg.DATABASE_PARAMS_DBNAME,
        "user": cfg.DATABASE_PARAMS_USERNAME,
        "password": cfg.DATABASE_PARAMS_PASSWORD,
    };
}

function makeConnection(cfg = process.env) {
    return mysql.createConnection(makeConfig(cfg));
}

exports.getPk = getPk;
exports.getTables = getTables;
exports.makeConnection = makeConnection;
exports.makeConfig = makeConfig;
exports.dbDir = function(conn) {
    const rtn = `./${conn.config.host}.${conn.config.database}`
    fs.mkdirSync(rtn, {recursive: true});
    return rtn;
}