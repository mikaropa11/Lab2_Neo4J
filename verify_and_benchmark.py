"""
Check the Cypher queries against the original SQL, and time them.

    python verify_and_benchmark.py --tpch ../tpc-h              # verify only
    python verify_and_benchmark.py --tpch ../tpc-h --benchmark  # + timing

1. verify: DuckDB runs the SQL of the statement over the .tbl files, and
   every Cypher result must give the same rows (numbers with a small
   tolerance, because of floating point).
2. benchmark: every query runs once to warm the cache and then --runs
   times; we keep the median time and the db hits of PROFILE. First
   without indexes, then with the indexes of queries.INDEXES. At the end
   the indexes are dropped again, so the database is left as it was.

The results are written to results/benchmark.csv.
"""
import argparse
import csv
import math
import os
import statistics
import time

import duckdb
from dotenv import load_dotenv
from neo4j import GraphDatabase

from queries import CYPHER, INDEXES, PARAMS, SQL

TABLES = {
    "region": "r_regionkey r_name r_comment",
    "nation": "n_nationkey n_name n_regionkey n_comment",
    "supplier": "s_suppkey s_name s_address s_nationkey s_phone s_acctbal s_comment",
    "customer": "c_custkey c_name c_address c_nationkey c_phone c_acctbal c_mktsegment c_comment",
    "part": "p_partkey p_name p_mfgr p_brand p_type p_size p_container p_retailprice p_comment",
    "partsupp": "ps_partkey ps_suppkey ps_availqty ps_supplycost ps_comment",
    "orders": "o_orderkey o_custkey o_orderstatus o_totalprice o_orderdate o_orderpriority "
              "o_clerk o_shippriority o_comment",
    "lineitem": "l_orderkey l_partkey l_suppkey l_linenumber l_quantity l_extendedprice "
                "l_discount l_tax l_returnflag l_linestatus l_shipdate l_commitdate "
                "l_receiptdate l_shipinstruct l_shipmode l_comment",
}
INTEGERS = ("key", "size", "shippriority", "linenumber", "availqty")
DECIMALS = ("acctbal", "price", "quantity", "discount", "tax", "supplycost")


def column_type(name):
    if name.endswith(INTEGERS):
        return "BIGINT"
    if name.endswith(DECIMALS):
        return "DOUBLE"
    if name.endswith("date"):
        return "DATE"
    return "VARCHAR"


def load_duckdb(folder):
    """One DuckDB table per .tbl (or .tbl.gz) file."""
    con = duckdb.connect()
    for table, columns in TABLES.items():
        path = os.path.join(folder, table + ".tbl")
        path = path + ".gz" if os.path.exists(path + ".gz") else path
        names = columns.split() + ["extra"]  # every line ends with '|'
        spec = ", ".join(f"'{n}': '{column_type(n)}'" for n in names)
        con.execute(f"CREATE TABLE {table} AS SELECT * EXCLUDE (extra) FROM read_csv("
                    f"'{path}', delim='|', header=false, columns={{{spec}}})")
    return con


def normalise(value):
    """Neo4j dates -> datetime.date, so they compare with DuckDB's."""
    return value.to_native() if hasattr(value, "to_native") else value


def same_rows(expected, got):
    if len(expected) != len(got):
        return False
    for row_e, row_g in zip(expected, got):
        for a, b in zip(row_e, row_g):
            if isinstance(a, float) or isinstance(b, float):
                if not math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-6):
                    return False
            elif a != b:
                return False
    return True


def run_cypher(session, query, params):
    return [[normalise(v) for v in record.values()]
            for record in session.run(query, params)]


def verify(session, con):
    print("Verification against DuckDB")
    for q in CYPHER:
        expected = [list(r) for r in con.execute(SQL[q].format(**PARAMS[q])).fetchall()]
        got = run_cypher(session, CYPHER[q], PARAMS[q])
        status = "OK  " if same_rows(expected, got) else "DIFF"
        print(f"  {q}: {status} ({len(got)} rows in Neo4j, {len(expected)} in SQL)")


def db_hits(plan):
    """Sum the db hits of every operator of a PROFILE plan."""
    own = plan.get("dbHits", 0)
    return own + sum(db_hits(child) for child in plan.get("children", []))


def time_queries(session, runs, phase):
    rows = []
    for q in CYPHER:
        session.run(CYPHER[q], PARAMS[q]).consume()          # warm-up
        times = []
        for _ in range(runs):
            start = time.perf_counter()
            list(session.run(CYPHER[q], PARAMS[q]))          # fetch every row
            times.append((time.perf_counter() - start) * 1000)
        result = session.run("PROFILE " + CYPHER[q], PARAMS[q])
        list(result)                                         # PROFILE needs every row read
        hits = db_hits(result.consume().profile)
        median = statistics.median(times)
        print(f"  {q}: {median:8.1f} ms   {hits:>10,} db hits")
        rows.append({"phase": phase, "query": q, "median_ms": round(median, 1),
                     "min_ms": round(min(times), 1), "max_ms": round(max(times), 1),
                     "db_hits": hits})
    return rows


def set_indexes(session, create):
    for name, target in INDEXES.items():
        if create:
            session.run(f"CREATE INDEX {name} IF NOT EXISTS {target}").consume()
        else:
            session.run(f"DROP INDEX {name} IF EXISTS").consume()
    if create:
        session.run("CALL db.awaitIndexes(300)").consume()   # wait until they are online


def benchmark(session, runs):
    set_indexes(session, create=False)
    print(f"\nWithout indexes (median of {runs} runs)")
    rows = time_queries(session, runs, "no_index")
    set_indexes(session, create=True)
    print(f"\nWith indexes (median of {runs} runs)")
    rows += time_queries(session, runs, "index")
    set_indexes(session, create=False)

    os.makedirs("results", exist_ok=True)
    with open("results/benchmark.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print("\nSaved in results/benchmark.csv")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tpch", default="../tpc-h", help="folder with the .tbl(.gz) files")
    parser.add_argument("--benchmark", action="store_true", help="also time the queries")
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    load_dotenv()
    driver = GraphDatabase.driver(os.getenv("NEO4J_URI", "neo4j://localhost:7687"),
                                  auth=(os.getenv("NEO4J_USERNAME", "neo4j"),
                                        os.getenv("NEO4J_PASSWORD")))
    with driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
        verify(session, load_duckdb(args.tpch))
        if args.benchmark:
            benchmark(session, args.runs)
    driver.close()


if __name__ == "__main__":
    main()
