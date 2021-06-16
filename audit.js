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
    .option('suffix', {description: 'Suffix for data dirs', type: 'string', default: ""})
    .option('process-tables', {description: 'Name of table to checksum', type: 'array'})
    .help().alias('help', 'h')
    .argv;

const srcCon = dbtools.makeConnection();

srcCon.on(`error`, (err) => console.error(`Connection error ${err.code}`));

async function checksum_table(conn, table, limit = null, checksum = "crc32", progress = null) {
    const md5s = (await Promise.all(Object.values(table.cols).map( (x) => dbtools.checksum_field(x, checksum)))).join(', ');
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

    var qstart = new Date()
    queries.write(sql); queries.write('\n\n');

    const results = conn.query(sql);
    var qtime = new Date() - qstart;
    //console.log(`${table.name} Query took ${qtime / 1000.}s`);

    var hstart = new Date()

    let w = zlib.createGzip();
    w.pipe(fs.createWriteStream(`${dbtools.dbDir(srcCon, argv['suffix'])}/${table.name}.csv.gz`));

    let original_progress = progress.value;
    let count = 0;
    try {
        const datacheck = await new Promise((resolve, error) => {
            let datacheck = crc32.calculate('');
            results.on('error', function (err) {
                console.log("Error while running ", sql);
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
                if(count % 1000 === 0)
                    progress.increment(1000, { table: table.name});

            }).on('end', function () {
                resolve(datacheck);
            });
        });
        progress.update(original_progress + count, {table: table.name})

        await w.end();

        var htime = new Date() - hstart;
        //console.log(`Hash took ${htime / 1000.}s`);

        return {
            datacheck: datacheck,
            count: count,
            qtime: qtime,
            htime: htime,
        };
    } catch (e) {
        console.log("Error while running ", sql);
        throw e;
    }

}

let queries = fs.createWriteStream(`./${dbtools.dbDir(srcCon, argv['suffix'])}/queries.csv`);

async function checksum_db(srcCon, limit = null, checksum = "crc32") {
    const table_objects = await dbtools.getTables(srcCon);
    let tables = Object.values(table_objects);
    if(argv['process-tables']) {
        tables = argv['process-tables'].map(n => table_objects[n]);
    }

    tables.sort((t) => t.name)

    const progress = new cliProgress.SingleBar({
        format: 'progress [{bar}] {percentage}% | {duration_formatted} / {eta}s | {value}/{total} | {table}',
        stream: process.stdout,
        forceRedraw: true,
        etaBuffer: 100000
    }, cliProgress.Presets.shades_classic);
    progress.on('redraw-post', () => { process.stdout.write("\r"); });

    progress.start(27500000, 0);

    const o = fs.createWriteStream(`./${dbtools.dbDir(srcCon, argv['suffix'])}/tables.csv`);
    let rtn = [];
    for(let i = 0;i < tables.length;i++) {
        const table = tables[i];
        const start = new Date();

        progress.update(null, {table: table.name});
        const results = await checksum_table(srcCon, table, limit, 'crc32', progress);
        if (results === null) {
            continue;
        }
        const local_limit = results.count;
        const time = new Date() - start;

        process.stdout.clearLine();
        console.log(printf('%-48s %16s %10d rows %8.3f krows / sec %7.3fs',
            table.name, results.datacheck, local_limit, local_limit / time, time / 1000.))
        progress.update(null, {table: table.name});

        o.write(printf("%-48s\t%16s\t%10d\n", table.name, results.datacheck, results.count));
        rtn.push({
            table: table.name,
            datacheck: results.datacheck,
            count: local_limit
        });
    }

    await o.end();
    console.log(`Processed ${progress.value} rows`)
    progress.stop()

    return rtn;
}

console.log(`Auditing ${srcCon.config.host}:${srcCon.config.port}`);

(async () =>
{
    await srcCon.awaitQuery(`SET SESSION group_concat_max_len = 100000;`)

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