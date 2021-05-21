require('dotenv').config()
const fs = require('fs');
const mysql = require(`mysql-await`);
const yargs = require('yargs');
const _ = require('lodash');
const cliProgress = require('cli-progress');
const dbtools = require('./dbtools')
const crypto = require('crypto')
const zlib = require('zlib')

const printf = require('fast-printf').printf

var crc32 = require('fast-crc32c');

const argv = yargs
    .option('limit', {description: 'Limit number of rows per query', type: 'int', default: null})
    .help().alias('help', 'h')
    .argv;

const srcCon = dbtools.makeConnection();

srcCon.on(`error`, (err) => console.error(`Connection error ${err.code}`));

async function checksum_field(col, checksum = "crc32") {
    let arg = `\`${await col.COLUMN_NAME}\``;

    const stringify_types = [
        'bigint',
        'int',
        'mediumint',
        'smallint',
        'tinyint',
        'float',
        'double',
        'real'
    ]

    if(col.IS_NULLABLE !== "NO") {
        // Effectively give null a unique constant value (Doesn't look like 0 or empty string)
        arg = `IFNULL(${arg}, '@9mT&$5CLZ!pd2Q$hxTG46Y')`;
    }

    if(stringify_types.indexOf(col.DATA_TYPE.toLowerCase()) >= 0) {
        arg = `CONVERT(${arg}, char)`;
    }

    return `${checksum}(${arg})`
}

async function checksum_table(conn, table, limit = null, checksum = "crc32") {
    const md5s = (await Promise.all(Object.values(table.cols).map( (x) => checksum_field(x, checksum)))).join(', ');
    let pkCols = dbtools.getPk(table);
    if (pkCols.length > 1) {
        //console.warn(`${table.name} has too many primary keys; skipping`);
        return null;
    }
    if (pkCols.length === 0) {
        //console.warn(`${table.name} has no primary key; skipping`);
        return null;
    }

    const pkNames = pkCols.map(col => `\`${col.COLUMN_NAME}\``).join(', ');

    const whereClause = '';
    const limitClause = limit ? `limit ${limit}` : '';
    const sql = `select ${pkNames} as id, md5(concat(${md5s})) as hash from \`${table.name}\` ${whereClause} order by ${pkNames} ${limitClause}`;
    //console.log("Running", sql)
    var qstart = new Date()
    const results = conn.query(sql);
    var qtime = new Date() - qstart;
    //console.log(`${table.name} Query took ${qtime / 1000.}s`);

    var hstart = new Date()

    let w = zlib.createGzip();
    w.pipe(fs.createWriteStream(`./${conn.config.host}.${conn.config.database}/${table.name}.csv.gz`));

    let count = 0;
    const datacheck = await new Promise( (resolve, error) => {
        let datacheck = crc32.calculate('');
        results.on('error', function (err) {
            error(err);
        }).on('fields', function (fields) {

        }).on('result', function (curr) {
            count++;
            const id = String(curr['id']);
            const hash = curr['hash'];
            if (checksum === 'md5') {
                datacheck.update(id);
                datacheck.update(hash);
            } else {
                datacheck = crc32.calculate(id, datacheck);
                datacheck = crc32.calculate(hash, datacheck);
            }
            w.write(`${id}\t${hash}\n`)
        }).on('end', function () {
            resolve(datacheck);
        });
    });

    await w.end();

    var htime = new Date() - hstart;
    //console.log(`Hash took ${htime / 1000.}s`);

    return {
        datacheck: datacheck,
        count: count,
        qtime: qtime,
        htime: htime,
    };
}

async function checksum_db(srcCon, limit = null, checksum = "crc32") {
    const tables = Object.values(await dbtools.getTables(srcCon));
    tables.sort((t) => t.name)

    const progress = new cliProgress.SingleBar({
        format: 'progress [{bar}] {percentage}% | ETA: {eta}s | {value}/{total} | {table}',
        stream: process.stdout,
        forceRedraw: true
    }, cliProgress.Presets.shades_classic);
    progress.on('redraw-post', () => { process.stdout.write("\r"); });

    progress.start(tables.length, 0);

    fs.mkdirSync(`./${srcCon.config.host}.${srcCon.config.database}`, {recursive: true});

    const o = fs.createWriteStream(`./${srcCon.config.host}.${srcCon.config.database}/tables.csv`);
    let rtn = [];
    for(let i = 0;i < tables.length;i++) {
        const table = tables[i];
        const start = new Date();

        progress.update(i, {table: table.name});
        const results = await checksum_table(srcCon, table, limit, 'crc32');
        if (results === null) {
            continue;
        }
        const local_limit = results.count;
        const time = new Date() - start;

        process.stdout.clearLine();
        console.log(printf('%-48s %16s %10d rows %8.3f krows / sec %7.3fs',
            table.name, results.datacheck, local_limit, local_limit / time, time / 1000.))
        progress.update(i, {table: table.name});

        o.write(printf("%-48s\t%16s\t%10d\n", table.name, results.datacheck, results.count));
        rtn.push({
            table: table.name,
            datacheck: results.datacheck,
            count: local_limit
        });
    }

    await o.end();
    progress.stop()

    return rtn;
}

console.log(`Auditing ${srcCon.config.host}:${srcCon.config.port}`);

(async () =>
{
    let limit = argv['limit'];
    if(limit !== null) {
        console.log(`NOTE: Limiting queries to ${limit}`)
    }
    var start = new Date()
    const results = await checksum_db(srcCon, limit,'crc32')
    const time = new Date() - start;

    console.log('Runtime %dmins', time / 1000. / 60.)

    await srcCon.awaitEnd();
})().catch((ex) => {
    console.error("Error synchronizing databases!", ex);
    srcCon
        .awaitEnd()
        .then(() => dstCon.awaitEnd())
        .finally(() => {
            process.exit(1);
        });
});