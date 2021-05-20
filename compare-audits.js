const dotenv = require('dotenv')
const fs = require('fs');
const mysql = require(`mysql-await`);
const yargs = require('yargs');
const _ = require('lodash');
const cliProgress = require('cli-progress');
const dbtools = require('./dbtools')
const crypto = require('crypto')
const zlib = require('zlib')
const readline = require('readline');
const { once } = require('events');

const printf = require('fast-printf').printf

var crc32 = require('fast-crc32c');

const env_a = dbtools.makeConnection(dotenv.parse(fs.readFileSync(process.argv[2])));
const env_b = dbtools.makeConnection(dotenv.parse(fs.readFileSync(process.argv[3])));

function intersect(o1, o2){
    const rtn = Object.keys(o1).filter(k => k in o2)
    rtn.sort()
    return rtn;
}

async function parseTableFile(fn) {
    const contents = await fs.readFileSync(fn, "utf8");
    const arrays = contents.split('\n').map( (line) => line.split("\t").map( x=>x.trim()));
    const rtn = {};
    arrays.forEach( line => {
        rtn[line[0]] = {
            checksum: line[1],
            count: line[2]
        }
    });
    return rtn;
}

function loadRowInfo(conn, table) {
    const rl = readline.createInterface({
        input: fs.createReadStream(`${dbtools.dbDir(conn)}/${table}.csv.gz`).pipe(zlib.createGunzip()),
        crlfDelay: Infinity
    });

/*
    const rtn = {};
    rl.on('line', (line) => {
        const vals = line.split("\t").map(s=>s.trim());
        rtn[parseInt(vals[0])] = vals[1];
    });

    await once(rl, 'close');
*/
    return rl;
}

async function investigate_table(table) {
    const rowsA = loadRowInfo(env_a, table.name);
    const rowsB = loadRowInfo(env_b, table.name);

    const itA = rowsA[Symbol.asyncIterator]();
    const itB = rowsB[Symbol.asyncIterator]();

    let lineA = null, lineB = null;

    async function extract(it) {
        let rtn = (await it.next()).value;
        if(rtn) {
            return rtn.split("\t").map(s => s.trim());
        }
        return null;
    }

    let aNotInB = 0;
    let bNotInA = 0;
    let lowestANotInB = Infinity, lowestBNotInA = Infinity;
    let maxA = 0, maxB = 0;

    const mismatches = [];
    lineA = await extract(itA);
    lineB = await extract(itB);
    do {
        if (lineA === null && lineB === null) {
            break;
        } else if (lineB !== null && (lineA === null || lineA[0] > lineB[0])) {
            bNotInA += 1;
            lowestBNotInA = Math.min(lineB[0], lowestBNotInA);
            lineB = await extract(itB);
            if(lineB !== null)
                maxB = Math.max(maxB, lineA[0]);
        } else if (lineA !== null && (lineB === null || lineB[0] > lineA[0])) {
            aNotInB += 1;
            lowestANotInB = Math.min(lineA[0], lowestANotInB);
            lineA = await extract(itA);
            if(lineA !== null)
                maxA = Math.max(maxA, lineA[0]);
        } else if (lineA[0] === lineB[0]) {
            if(lineA[1] !== lineB[1]) {
                mismatches.push(lineA[0]);
            }
            lineB = await extract(itB);
            if(lineB !== null)
                maxB = Math.max(maxB, lineA[0]);

            lineA = await extract(itA);
            if(lineA !== null)
                maxA = Math.max(maxA, lineA[0]);
        }
    } while(true);

    console.log(printf("%-48s %7d a-not-in-b (%7d low vs %7d) %7d a-not-in-b (%7d low vs %7d) %7d bad checksums", table.name,
        aNotInB, lowestANotInB, maxB,
        bNotInA, lowestBNotInA, maxA, mismatches.length));

    let pkCols = dbtools.getPk(table);

    if(mismatches.length) {
        const show_cnt = 5;
        const q = `select * from ${table.name} where ${pkCols[0].COLUMN_NAME} in (${mismatches.slice(0,show_cnt)});`
        const results_a = await env_a.awaitQuery(q);
        const results_b = await env_b.awaitQuery(q);

        for(let idx = 0;idx < results_a.length;idx++) {
            Object.keys(results_a[idx]).forEach(k=>{
                let isEqual = (results_a[idx][k] != null && results_a[idx][k].equals) ? results_a[idx][k].equals(results_b[idx][k]) :
                    results_a[idx][k] === results_b[idx][k];
                if(results_a[idx][k] && results_b[idx][k] && results_a[idx][k].getTime && results_b[idx][k].getTime) {
                    isEqual = results_a[idx][k].getTime() === results_b[idx][k].getTime();
                }
                if( !isEqual) {
                    var auxA = results_a[idx][k].getTime ? results_a[idx][k].getTime() : '';
                    var auxB = results_a[idx][k].getTime ? results_a[idx][k].getTime() : '';
                    console.log(`\t${table.name}[${mismatches[idx]}]::${k} '${results_a[idx][k]}' ${auxA} vs '${results_b[idx][k]}' ${auxB}`)
                }
            })
        }
    }
    return 0;
}

(async () =>
{
    var tables_a = await parseTableFile(`${dbtools.dbDir(env_a)}/tables.csv`)
    var tables_b = await parseTableFile(`${dbtools.dbDir(env_b)}/tables.csv`);
    var common_tables = intersect(tables_a, tables_b);
    const table_defs = await dbtools.getTables(env_a);

    for(const table of common_tables) {
        if(tables_a[table].count !== tables_b[table].count) {
            console.warn(`Miscount error for ${table} ${tables_a[table].count} vs ${tables_b[table].count}`)
            await investigate_table(table_defs[table]);
        } else if(tables_a[table].checksum !== tables_b[table].checksum) {
            console.error(`Bad checksum for ${table} ${tables_a[table].checksum} vs ${tables_b[table].checksum}`)
            await investigate_table(table_defs[table]);
        }
    }

    await env_a.awaitEnd();
    await env_b.awaitEnd();
})().catch(async (ex) => {
    await env_a.awaitEnd();
    await env_b.awaitEnd();
});
