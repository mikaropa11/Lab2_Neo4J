"""
Alternative model (Section 7 of the report): LineItem as a RELATIONSHIP.

    Final model:   (:Order)-[:CONTAINS]->(:LineItem)-[:SUPPLIED_BY]->(:Supplier)
    Alternative:   (:Order)-[:LINE {quantity, extendedprice, ...}]->(:Supplier)

Each line belongs to exactly one order and one supplier, so it could be a
relationship. This script:
  1. times the final model as it is (without and with indexes);
  2. builds the LINE relationships from the LineItem nodes already loaded;
  3. checks the alternative queries against DuckDB and times them;
  4. deletes the LINE relationships (--keep to leave them).
The final model is timed BEFORE LINE exists: an Order with few relationships
keeps them in one chain, so extra LINE relationships would slow down its
CONTAINS expansion. The alternative still has CONTAINS next to LINE, so its
Q3/Q4 numbers are, if anything, slightly pessimistic.

    python alternative_lineitem_relationship.py --tpch ../tpch-data

Q2 does not use line items, so it is the same in both models and is not
repeated here. Results: results/alternative_lineitem.csv
"""
import argparse
import csv
import os

from dotenv import load_dotenv
from neo4j import GraphDatabase

from queries import CYPHER, INDEXES
from verify_and_benchmark import load_duckdb, set_indexes, time_queries, verify

ALTERNATIVE = {
    "q1": """
        MATCH ()-[l:LINE]->()
        WHERE l.shipdate <= date($date)
        RETURN l.returnflag, l.linestatus,
               sum(l.quantity) AS sum_qty,
               sum(l.extendedprice) AS sum_base_price,
               sum(l.extendedprice * (1 - l.discount)) AS sum_disc_price,
               sum(l.extendedprice * (1 - l.discount) * (1 + l.tax)) AS sum_charge,
               avg(l.quantity) AS avg_qty,
               avg(l.extendedprice) AS avg_price,
               avg(l.discount) AS avg_disc,
               count(*) AS count_order
        ORDER BY l.returnflag, l.linestatus""",

    "q3": """
        MATCH (c:Customer)-[:MAKES]->(o:Order)-[l:LINE]->()
        WHERE c.mktsegment = $segment
          AND o.orderdate < date($date)
          AND l.shipdate > date($date)
        RETURN o.id, sum(l.extendedprice * (1 - l.discount)) AS revenue,
               o.orderdate, o.shippriority
        ORDER BY revenue DESC, o.orderdate""",

    "q4": """
        MATCH (r:Region)<-[:IN_REGION]-(n:Nation)<-[:CUSTOMER_IN]-(c:Customer)
              -[:MAKES]->(o:Order)-[l:LINE]->(s:Supplier)-[:SUPPLIER_IN]->(n)
        WHERE r.name = $region
          AND o.orderdate >= date($date)
          AND o.orderdate < date($date) + duration({years: 1})
        RETURN n.name, sum(l.extendedprice * (1 - l.discount)) AS revenue
        ORDER BY revenue DESC""",
}
FINAL = {q: CYPHER[q] for q in ALTERNATIVE}

# Same indexes as the final model, but shipdate now lives on a relationship.
ALT_INDEXES = {name: target for name, target in INDEXES.items() if name != "lineitem_shipdate"}
ALT_INDEXES["line_shipdate"] = "FOR ()-[r:LINE]-() ON (r.shipdate)"

BUILD = """
    MATCH (o:Order)-[:CONTAINS]->(l:LineItem)-[:SUPPLIED_BY]->(s:Supplier)
    CALL (o, l, s) {
        CREATE (o)-[:LINE {linenumber: l.linenumber, quantity: l.quantity,
                           extendedprice: l.extendedprice, discount: l.discount, tax: l.tax,
                           returnflag: l.returnflag, linestatus: l.linestatus,
                           shipdate: l.shipdate}]->(s)
    } IN TRANSACTIONS OF 50000 ROWS"""

DROP = """
    MATCH ()-[r:LINE]->()
    CALL (r) { DELETE r } IN TRANSACTIONS OF 50000 ROWS"""

# Orders with two or more lines from the same supplier: parallel LINE relationships.
PARALLEL = """
    MATCH (o:Order)-[l:LINE]->(s:Supplier)
    WITH o, s, count(l) AS lines WHERE lines > 1
    RETURN count(*) AS pairs, sum(lines) AS lines"""


def measure(session, runs, model, queries, indexes):
    """Without indexes, then with indexes; indexes are dropped at the end."""
    set_indexes(session, create=False, indexes=indexes)
    print(f"\n{model}, without indexes")
    rows = time_queries(session, runs, f"no_index/{model}", queries)
    set_indexes(session, create=True, indexes=indexes)
    print(f"{model}, with indexes")
    rows += time_queries(session, runs, f"index/{model}", queries)
    set_indexes(session, create=False, indexes=indexes)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tpch", default="../tpc-h", help="folder with the .tbl(.gz) files")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--keep", action="store_true", help="do not delete LINE at the end")
    args = parser.parse_args()

    load_dotenv()
    driver = GraphDatabase.driver(os.getenv("NEO4J_URI", "neo4j://localhost:7687"),
                                  auth=(os.getenv("NEO4J_USERNAME", "neo4j"),
                                        os.getenv("NEO4J_PASSWORD")))
    with driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
        session.run(DROP).consume()                     # start clean if a run was interrupted
        set_indexes(session, create=False, indexes=ALT_INDEXES)
        rows = measure(session, args.runs, "node", FINAL, INDEXES)

        print("\nCreating the LINE relationships (about 1 minute)...")
        session.run(BUILD).consume()
        n = session.run("MATCH ()-[r:LINE]->() RETURN count(r) AS n").single()["n"]
        par = session.run(PARALLEL).single()
        print(f"  {n:,} LINE relationships; {par['pairs']:,} order-supplier pairs "
              f"with parallel relationships ({par['lines']:,} lines)")
        verify(session, load_duckdb(args.tpch), ALTERNATIVE)
        rows += measure(session, args.runs, "relationship", ALTERNATIVE, ALT_INDEXES)

        if not args.keep:
            session.run(DROP).consume()
            print("\nLINE relationships deleted: the database is as before.")
    driver.close()

    os.makedirs("results", exist_ok=True)
    with open("results/alternative_lineitem.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print("Saved in results/alternative_lineitem.csv")


if __name__ == "__main__":
    main()
