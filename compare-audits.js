const dotenv = require('dotenv')
dotenv.config()

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

const argv = yargs
    .option('process-tables',  {description: 'list of tables to process', type: 'array'})
    .option('ignore-null-vs-empty', {description: 'ignore null vs empty', type: 'bool'})
    .option('ignore-null-vs-zero-date', {description: 'ignore null vs zero date', type: 'bool'})
    .help().alias('help', 'h')
    .argv;

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

let queries = fs.createWriteStream(`./${dbtools.dbDir(env_a, argv['suffix'])}/compare_queries.csv`);

async function investigate_table(table_a, table_b) {
    const table = table_a;
    const rowsA = loadRowInfo(env_a, table.name);
    const rowsB = loadRowInfo(env_b, table.name);

    const itA = rowsA[Symbol.asyncIterator]();
    const itB = rowsB[Symbol.asyncIterator]();

    let lineA = null, lineB = null;

    async function extract(it, curr) {
        let rtn = (await it.next()).value;
        if(rtn) {
            rtn = rtn.split("\t").map(s => s.trim())
            return curr = {
                id: rtn[0],
                checksum: rtn[1],
                max: Math.max(rtn[0], curr === null ? 0 : curr.max)
            }
        }
        return {id: null, max: curr ? curr.max : 0};
    }

    let aNotInB = 0;
    let bNotInA = 0;
    let lowestANotInB = Infinity, lowestBNotInA = Infinity;
    let highestANotInB = 0, highestBNotInA = 0;

    const mismatches = [];

    lineA = await extract(itA, lineA);
    lineB = await extract(itB, lineB);

    while(lineA.id !== null || lineB.id !== null) {
        if (lineB.id !== null && (lineA.id === null || lineA.id > lineB.id)) {
            bNotInA += 1;
            lowestBNotInA = Math.min(lineB.id, lowestBNotInA);
            highestBNotInA = Math.max(lineB.id, highestBNotInA);
            lineB = await extract(itB, lineB);
        } else if (lineA.id !== null && (lineB.id === null || lineB.id > lineA.id)) {
            aNotInB += 1;
            lowestANotInB = Math.min(lineA.id, lowestANotInB);
            highestANotInB = Math.max(lineA.id, highestANotInB);
            lineA = await extract(itA, lineA);
        } else if (lineA.id === lineB.id) {
            if(lineA.checksum !== lineB.checksum) {
                mismatches.push({id: lineA.id, checksum_a: lineA.checksum, checksum_b: lineB.checksum});
            }
            lineB = await extract(itB, lineB);
            lineA = await extract(itA, lineA);
        } else {
            throw "Whats the other option?";
        }
    }

    if(aNotInB || bNotInA) {
        function print_case(name, other_name, aNotInB, lowestANotInB, highestANotInB, other_max) {
            if(lowestANotInB > other_max) {
                console.log(printf("%-48s %7d new records in %s but not %s", table.name, aNotInB, name, other_name));
            } else {
                console.log(printf("%-48s %7d records only in %s in range [%7d..%7d.] Max %s is %7d", table.name, aNotInB, name, lowestANotInB, highestANotInB, other_name, other_max));
            }
        }
        if(aNotInB) { print_case("A", "B", aNotInB, lowestANotInB, highestANotInB, lineB.max) }
        if(bNotInA) { print_case("B", "A", bNotInA, lowestBNotInA, highestBNotInA, lineA.max) }
    }

    if(mismatches.length) {
        let pkCols = dbtools.getPk(table);
        const common_rows = intersect(table_a.cols, table_b.cols);

        const extra_a = Object.keys(table_a.cols).filter(x => common_rows.indexOf(x) < 0);
        const extra_b = Object.keys(table_b.cols).filter(x => common_rows.indexOf(x) < 0);

        let print_count = 50;
        let batch_size = 1000;
        let ignored = 0;
        let floatIssues = 0;
        const ignored_column_set = new Set();

        const column_set = [
            ...common_rows,
            ...await dbtools.checksum_fields(table.cols, '_calc', ''),
            ...await dbtools.checksum_fields(table.cols, '_hash'),
            `md5(concat(${await dbtools.checksum_fields(table.cols)})) as full_table_hash`
        ]

        queries.write(`select ${column_set.join((", "))} from ${table.name}\n\n`)
        for(let offset = 0;offset < mismatches.length && print_count;offset += batch_size) {
            const q = `select ${column_set.join((", "))} from ${table.name} where ${pkCols[0].COLUMN_NAME} in (${mismatches.slice(offset, offset + batch_size).map(x => x.id).join(',')});`
            const results_a = await env_a.awaitQuery(q);
            const results_b = await env_b.awaitQuery(q);

            for (let idx = 0; idx < results_a.length && print_count; idx++) {
                const unequalSet = new Set();
                let foundIssue = false;
                Object.keys(results_a[idx]).forEach(k => {
                    if(unequalSet.has(k))
                        return;

                    const value_a = results_a[idx][k];
                    const value_b = results_b[idx][k];

                    let isEqual = (value_a != null && value_a.equals) ? value_a.equals(value_b) :
                        value_a === value_b;
                    if (value_a && value_b && value_a.getTime && value_b.getTime) {
                        isEqual = value_a.getTime() === value_b.getTime();
                    }

                    var float_regexp = /^-?\d+\.?\d*$/;

                    if(!isEqual) {
                        foundIssue = true;
                        if(k.endsWith('_calc') && float_regexp.test(value_a) && float_regexp.test(value_b)) {
                            const flt_a = parseFloat(value_a), flt_b = parseFloat(value_b);
                            const diff = Math.abs(flt_a - flt_b);
                            isEqual = diff < Math.abs(flt_a + flt_b) * .001;
                            //console.log(flt_a, flt_b, isEqual, diff, Math.abs(flt_a + flt_b) * .001)
                            floatIssues += isEqual;
                        }
                        const key = k.replace("_hash", "").replace("_calc", "");
                        unequalSet.add(`${key}_hash`)
                        unequalSet.add(`${key}_calc`)
                        unequalSet.add(`full_table_hash`)
                    }

                    let valid_againt_nulls = [];
                    if(argv['ignore-null-vs-empty']) valid_againt_nulls.push('')
                    if(argv['ignore-null-vs-zero-date']) valid_againt_nulls.push("0000-00-00 00:00:00", "0000-00-00")

                    for(var v of valid_againt_nulls) {
                        if(!isEqual) {
                            isEqual = (value_a === null && value_b === v) ||
                                (value_b === null && value_a === v);
                            if(isEqual) { ignored += 1; ignored_column_set.add(k) }
                        }
                    }
                    if(!isEqual && argv['ignore-null-vs-zero-date']) {
                        isEqual = (value_a === null && value_b.getTime && value_b.getTime() === 0) ||
                            (value_b === null && value_a.getTime && value_a.getTime() === 0);
                        if(isEqual) { ignored += 1; ignored_column_set.add(k) }
                    }

                    if (!isEqual) {
                        var auxA = value_a.getTime ? value_a.getTime() : '';
                        var auxB = value_a.getTime ? value_a.getTime() : '';
                        console.log(`\t${table.name}[${mismatches[idx].id}]::${k} '${value_a}' vs '${value_b}'`)
                        print_count--;
                    }
                })

                if(!foundIssue) {
                    console.log(`Could not find the issue with ${table.name} ${JSON.stringify(mismatches[idx])} ${results_a[idx]['full_table_hash']} ${results_b[idx]['full_table_hash']}!`)
                }
            }
        }
        let ignored_msg = ""
        if(ignored) {
            ignored_msg = printf("%7d ignored columns (%s) ", ignored, Array.from(ignored_column_set).join(", "))
        }
        function schema_msg(name, extra_cols) {
            if(extra_cols.length) {
                return printf("%s has extra columns (%s) ", name, extra_cols.join(", "))
            }
            return "";
        }
        let float_msg = floatIssues ? printf('5%d float issues', floatIssues) : '';
        console.log(printf("%-48s %7d checksum row mismatches %s%s%s%s", table.name, mismatches.length, ignored_msg, schema_msg("A", extra_a), schema_msg("B", extra_b)), float_msg);
    }
    return 0;
}

(async () =>
{
    var tables_a = await parseTableFile(`${dbtools.dbDir(env_a)}/tables.csv`)
    var tables_b = await parseTableFile(`${dbtools.dbDir(env_b)}/tables.csv`);
    let common_tables = new Set(intersect(tables_a, tables_b));

    if(argv['process-tables']) {
        common_tables = new Set(argv['process-tables']);
        console.log(`Only running tables ${Array.from(common_tables)}`)
    } else {
        var skip_tables = process.env.SKIP_TABLES;
        if (skip_tables) {
            for (const t of skip_tables.split(",")) {
                if (common_tables.has(t)) {
                    console.log(`Not checking ${t}; on skip list`)
                    common_tables.delete(t);
                }
            }
        }
    }

    const table_defs_a = await dbtools.getTables(env_a);
    const table_defs_b = await dbtools.getTables(env_b);

    for(const table of common_tables) {
        const miscount_error = tables_a[table].count !== tables_b[table].count;
        const checksum_error = tables_a[table].checksum !== tables_b[table].checksum;
        if(checksum_error || miscount_error) {
            await investigate_table(table_defs_a[table], table_defs_b[table]);
        }
    }

    await env_a.awaitEnd();
    await env_b.awaitEnd();
    await queries.end();
})().catch(async (ex) => {
    await env_a.awaitEnd();
    await env_b.awaitEnd();
    await queries.end();
    throw ex;
});