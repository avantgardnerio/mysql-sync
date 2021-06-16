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
exports.dbDir = function(conn, suffix = "") {
    const rtn = `./${conn.config.host}.${conn.config.database}${suffix}`
    fs.mkdirSync(rtn, {recursive: true});
    return rtn;
}

exports.checksum_field = async function(col, checksum = "crc32") {
    let arg = `\`${await col.COLUMN_NAME}\``;

    const stringify_types = [
        'bigint',
        'int',
        'mediumint',
        'smallint',
        'tinyint',
    ]
    const float_percision_tolerance = .001;
    if(['real', 'double', 'float'].indexOf(col.DATA_TYPE.toLowerCase()) >= 0) {
        arg = `IF(${arg} = 0, '0', 0+TRIM(TRUNCATE(${arg}, GREATEST(0, CEIL(ABS(LOG10(ABS(${arg} * ${float_percision_tolerance}))))))))`
    }

    if(col.IS_NULLABLE !== "NO") {
        // Effectively give null a unique constant value (Doesn't look like 0 or empty string)
        arg = `IFNULL(${arg}, '@9mT&$5CLZ!pd2Q$hxTG46Y')`;
    }
    /*
        if(stringify_types.indexOf(col.DATA_TYPE.toLowerCase()) >= 0) {
            arg = `CONVERT(${arg}, char)`;
        }
    */
    return `${checksum}(${arg})`
}


exports.checksum_fields = async function(cols, named_suffix = '', checksum = "crc32") {
    function generate_suffix(n) {
        if(named_suffix === '')
            return '';
        return ` as ${n}${named_suffix}`
    }
    return (await Promise.all(
        Object.values(cols).map(
            async (x) => `${await exports.checksum_field(x, checksum)}${generate_suffix(await x.COLUMN_NAME)}`
        )
    ));
}