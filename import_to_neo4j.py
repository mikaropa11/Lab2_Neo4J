"""
Load the CSV files produced by transform_data.py into Neo4j.

Graph model:

    (:Region)<-[:IN_REGION]-(:Nation)
    (:Nation)<-[:CUSTOMER_IN]-(:Customer)-[:MAKES]->(:Order)-[:CONTAINS]->(:LineItem)
    (:LineItem)-[:SUPPLIED_BY]->(:Supplier)-[:SUPPLIER_IN]->(:Nation)
    (:Supplier)-[:SUPPLIES {supplycost}]->(:Part)

Connection settings come from a .env file:
    NEO4J_URI=neo4j://localhost:7687
    NEO4J_USERNAME=neo4j
    NEO4J_PASSWORD=...
    NEO4J_DATABASE=neo4j

Usage:
    python import_to_neo4j.py                 # load ./transformed-data
    python import_to_neo4j.py --reset         # delete everything first
"""
import argparse
import csv
import os
import sys
import time
from datetime import date
from itertools import islice
from pathlib import Path

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

URI = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD")
DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

BATCH_SIZE = 25_000

# Type of every property that is not text. "id" is always an integer.
INTS = {"id", "start_id", "end_id", "size", "shippriority", "linenumber"}
FLOATS = {"acctbal", "quantity", "extendedprice", "discount", "tax", "supplycost"}
DATES = {"orderdate", "shipdate"}

NODES = [  # (label, file); small tables first
    ("Region", "regions.csv"),
    ("Nation", "nations.csv"),
    ("Supplier", "suppliers.csv"),
    ("Customer", "customers.csv"),
    ("Part", "parts.csv"),
    ("Order", "orders.csv"),
    ("LineItem", "lineitems.csv"),
]

RELATIONSHIPS = [  # (type, source label, target label, file)
    ("IN_REGION", "Nation", "Region", "nation_in_region.csv"),
    ("CUSTOMER_IN", "Customer", "Nation", "customer_in_nation.csv"),
    ("SUPPLIER_IN", "Supplier", "Nation", "supplier_in_nation.csv"),
    ("MAKES", "Customer", "Order", "customer_makes_order.csv"),
    ("CONTAINS", "Order", "LineItem", "order_contains_lineitem.csv"),
    ("SUPPLIED_BY", "LineItem", "Supplier", "lineitem_supplied_by_supplier.csv"),
    ("SUPPLIES", "Supplier", "Part", "supplier_supplies_part.csv"),
]


def convert(row):
    """CSV text -> Python values that Neo4j stores with the right type."""
    result = {}
    for key, value in row.items():
        if key in INTS:
            result[key] = int(value)
        elif key in FLOATS:
            result[key] = float(value)
        elif key in DATES:
            result[key] = date.fromisoformat(value)   # stored as a Neo4j Date
        else:
            result[key] = value
    return result


def batches(path):
    """Yield the rows of a CSV file in lists of BATCH_SIZE converted rows."""
    with path.open("r", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        while batch := list(islice(reader, BATCH_SIZE)):
            yield [convert(row) for row in batch]


def create_constraints(session):
    """One uniqueness constraint per label on `id`.

    Neo4j creates an index for each one. They make the import fast (every
    relationship looks up its two nodes by id), but none of the four queries
    filters by id, so they do not affect the comparison without indexes.
    """
    for label, _ in NODES:
        session.run(f"CREATE CONSTRAINT {label.lower()}_id IF NOT EXISTS "
                    f"FOR (n:{label}) REQUIRE n.id IS UNIQUE").consume()


def import_nodes(session, label, path):
    query = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{id: row.id}})
    SET n += row
    """
    total = 0
    for rows in batches(path):
        session.execute_write(lambda tx, data=rows: tx.run(query, rows=data).consume())
        total += len(rows)
        print(f"  {label}: {total:,}", end="\r", flush=True)
    print(f"  {label}: {total:,} nodes")


def import_relationships(session, rel_type, source, target, path):
    # Only SUPPLIES has a property: the supply cost of the part-supplier pair.
    set_properties = "SET r.supplycost = row.supplycost" if rel_type == "SUPPLIES" else ""
    query = f"""
    UNWIND $rows AS row
    MATCH (a:{source} {{id: row.start_id}})
    MATCH (b:{target} {{id: row.end_id}})
    MERGE (a)-[r:{rel_type}]->(b)
    {set_properties}
    RETURN count(*) AS matched
    """
    total = matched = 0
    for rows in batches(path):
        matched += session.execute_write(
            lambda tx, data=rows: tx.run(query, rows=data).single()["matched"])
        total += len(rows)
        print(f"  {rel_type}: {matched:,}", end="\r", flush=True)
    print(f"  {rel_type} ({source}->{target}): {matched:,} relationships")
    if matched != total:
        print(f"  WARNING: {total - matched:,} rows did not find both nodes",
              file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Load the TPC-H graph into Neo4j.")
    parser.add_argument("--data", type=Path, default=Path("transformed-data"),
                        help="folder created by transform_data.py")
    parser.add_argument("--reset", action="store_true",
                        help="delete all nodes and relationships first")
    args = parser.parse_args()
    if not PASSWORD:
        sys.exit("Set NEO4J_PASSWORD in the .env file.")

    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
    start = time.perf_counter()
    try:
        with driver.session(database=DATABASE) as session:
            if args.reset:
                # Delete in small transactions, so a big graph fits in memory.
                session.run("MATCH (n) CALL (n) { DETACH DELETE n } "
                            "IN TRANSACTIONS OF 10000 ROWS").consume()
                print("Database emptied.")
            create_constraints(session)
            print("Nodes:")
            for label, file_name in NODES:
                import_nodes(session, label, args.data / "nodes" / file_name)
            print("Relationships:")
            for rel_type, source, target, file_name in RELATIONSHIPS:
                import_relationships(session, rel_type, source, target,
                                     args.data / "relationships" / file_name)
    finally:
        driver.close()
    print(f"Done in {time.perf_counter() - start:.0f} s")


if __name__ == "__main__":
    main()
