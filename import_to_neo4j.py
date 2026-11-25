import csv
import os
from dotenv import load_dotenv
import sys
from datetime import date
from itertools import islice
from pathlib import Path

from neo4j import GraphDatabase

load_dotenv()

URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")
DATABASE = os.getenv("NEO4J_DATABASE")

CSV_DIR = Path(__file__).resolve().parent / "transformed-data"
BATCH_SIZE = 25_000


NODE_CONFIG = [
    {
        "label": "Part",
        "file": "parts.csv",
        "ints": {"P_PARTKEY", "P_SIZE"},
        "floats": {"P_RETAILPRICE"},
        "dates": set(),
    },
    {
        "label": "Supplier",
        "file": "suppliers.csv",
        "ints": {"S_SUPPKEY", "S_NATIONKEY"},
        "floats": {"S_ACCTBAL"},
        "dates": set(),
    },
    {
        "label": "Customer",
        "file": "customers.csv",
        "ints": {"C_CUSTKEY", "C_NATIONKEY"},
        "floats": {"C_ACCTBAL"},
        "dates": set(),
    },
    {
        "label": "Order",
        "file": "orders.csv",
        "ints": {"O_ORDERKEY", "O_CUSTKEY", "O_SHIPPRIORITY"},
        "floats": {"O_TOTALPRICE"},
        "dates": {"O_ORDERDATE"},
    },
    {
        "label": "LineItem",
        "file": "lineitems.csv",
        "ints": {"L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER"},
        "floats": {"L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX"},
        "dates": {"L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE"},
    },
    {
        "label": "Nation",
        "file": "nations.csv",
        "ints": {"N_NATIONKEY", "N_REGIONKEY"},
        "floats": set(),
        "dates": set(),
    },
    {
        "label": "Region",
        "file": "regions.csv",
        "ints": {"R_REGIONKEY"},
        "floats": set(),
        "dates": set(),
    },
]

RELATIONSHIP_CONFIG = [
    {
        "type": "PLACED",
        "source": "Customer",
        "target": "Order",
        "file": "customer_placed_orders.csv",
        "property_ints": set(),
        "property_floats": set(),
    },
    {
        "type": "CONTAINS",
        "source": "Order",
        "target": "LineItem",
        "file": "order_contains_lineitems.csv",
        "property_ints": set(),
        "property_floats": set(),
    },
    {
        "type": "SPECIFIES",
        "source": "LineItem",
        "target": "Part",
        "file": "lineitem_specifies_parts.csv",
        "property_ints": set(),
        "property_floats": set(),
    },
    {
        "type": "SOURCED_FROM",
        "source": "LineItem",
        "target": "Supplier",
        "file": "lineitem_sourced_from_suppliers.csv",
        "property_ints": set(),
        "property_floats": set(),
    },
    {
        "type": "SUPPLIED_BY",
        "source": "Part",
        "target": "Supplier",
        "file": "part_supplied_by_supplier.csv",
        "property_ints": {"PS_AVAILQTY"},
        "property_floats": {"PS_SUPPLYCOST"},
    },
    {
        "type": "LOCATED_IN",
        "source": "Customer",
        "target": "Nation",
        "file": "customer_located_in_nation.csv",
        "property_ints": set(),
        "property_floats": set(),
    },
    {
        "type": "LOCATED_IN",
        "source": "Supplier",
        "target": "Nation",
        "file": "supplier_located_in_nation.csv",
        "property_ints": set(),
        "property_floats": set(),
    },
    {
        "type": "BELONGS_TO",
        "source": "Nation",
        "target": "Region",
        "file": "nation_belongs_to_region.csv",
        "property_ints": set(),
        "property_floats": set(),
    },
]


def convert_row(row, ints, floats, dates):
    """Convert CSV strings into Neo4j-compatible Python values."""
    converted = {}
    for key, value in row.items():
        if value is None or value == "":
            converted[key] = None
        elif key == "id" or key in {"start_id", "end_id"}:
            converted[key] = value
        elif key in ints:
            converted[key] = int(value)
        elif key in floats:
            converted[key] = float(value)
        elif key in dates:
            converted[key] = date.fromisoformat(value)
        else:
            converted[key] = value
    return converted


def batches(csv_path, batch_size):
    with csv_path.open("r", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        while True:
            batch = list(islice(reader, batch_size))
            if not batch:
                break
            yield batch


def create_constraints(session):
    constraints = [
        ("Part", "part_id"),
        ("Supplier", "supplier_id"),
        ("Customer", "customer_id"),
        ("Order", "order_id"),
        ("LineItem", "lineitem_id"),
        ("Nation", "nation_id"),
        ("Region", "region_id"),
    ]
    for label, name in constraints:
        session.run(
            f"CREATE CONSTRAINT {name} IF NOT EXISTS "
            f"FOR (n:{label}) REQUIRE n.id IS UNIQUE"
        ).consume()
    print("Uniqueness constraints are in place.")


def import_nodes(session, config):
    csv_path = CSV_DIR / "nodes" / config["file"]
    if not csv_path.is_file():
        raise FileNotFoundError(f"Node CSV not found: {csv_path}")

    label = config["label"]
    query = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{id: row.id}})
    SET n += row
    RETURN count(*) AS processed
    """

    total = 0
    for raw_batch in batches(csv_path, BATCH_SIZE):
        rows = [
            convert_row(raw, config["ints"], config["floats"], config["dates"])
            for raw in raw_batch
        ]
        result = session.execute_write(
            lambda tx, data=rows: tx.run(query, rows=data).single()["processed"]
        )
        total += result
        print(f"  {label}: {total:,} rows processed", end="\r", flush=True)

    print(f"  {label}: {total:,} rows processed.")
    return total


def import_relationships(session, config):
    csv_path = CSV_DIR / "relationships" / config["file"]
    if not csv_path.is_file():
        raise FileNotFoundError(f"Relationship CSV not found: {csv_path}")

    rel_type = config["type"]
    source = config["source"]
    target = config["target"]


    query = f"""
    UNWIND $rows AS row
    MATCH (a:{source} {{id: row.start_id}})
    MATCH (b:{target} {{id: row.end_id}})
    MERGE (a)-[r:{rel_type}]->(b)
    SET r += row.properties
    RETURN count(*) AS processed
    """

    total = 0
    rows_seen = 0
    for raw_batch in batches(csv_path, BATCH_SIZE):
        rows_seen += len(raw_batch)
        prepared = []
        for raw in raw_batch:
            properties = {}
            for key, value in raw.items():
                if key in {"start_id", "end_id"}:
                    continue
                if value == "":
                    properties[key] = None
                elif key in config["property_ints"]:
                    properties[key] = int(value)
                elif key in config["property_floats"]:
                    properties[key] = float(value)
                else:
                    properties[key] = value
            prepared.append({
                "start_id": raw["start_id"],
                "end_id": raw["end_id"],
                "properties": properties,
            })

        processed = session.execute_write(
            lambda tx, data=prepared: tx.run(query, rows=data).single()["processed"]
        )
        total += processed
        print(f"  {rel_type} ({source}->{target}): {total:,} rows matched", end="\r", flush=True)

    print(f"  {rel_type} ({source}->{target}): {total:,} rows matched.")
    if total != rows_seen:
        print(
            f"  WARNING: {rows_seen - total:,} of {rows_seen:,} rows did not "
            "match both endpoint nodes. Check the source IDs and import order.",
            file=sys.stderr,
        )
    return total, rows_seen


def main():
    if not CSV_DIR.is_dir():
        raise FileNotFoundError(
            f"Could not find {CSV_DIR}. Run transform_tpch.py first, "
            "or place transformed-data beside this script."
        )

    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
    try:
        driver.verify_connectivity()
        print(f"Connected to Neo4j at {URI}; database: {DATABASE}")

        with driver.session(database=DATABASE) as session:
            create_constraints(session)

            print("\nImporting nodes...")
            node_total = 0
            for config in NODE_CONFIG:
                node_total += import_nodes(session, config)

            print("\nImporting relationships...")
            relationship_total = 0
            relationship_rows = 0
            for config in RELATIONSHIP_CONFIG:
                imported, seen = import_relationships(session, config)
                relationship_total += imported
                relationship_rows += seen

            print("\nImport summary")
            print(f"  Node rows processed:         {node_total:,}")
            print(f"  Relationship rows in CSVs:   {relationship_rows:,}")
            print(f"  Relationships matched:      {relationship_total:,}")
            if relationship_total == relationship_rows:
                print("  All relationship endpoints matched successfully.")
            else:
                print("  Some relationships were skipped; inspect the warnings above.")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
