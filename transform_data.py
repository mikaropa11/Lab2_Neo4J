"""
Transform the TPC-H .tbl files into the CSV files that import_to_neo4j.py loads.

Graph model (see the report):

    (:Region)<-[:IN_REGION]-(:Nation)
    (:Nation)<-[:CUSTOMER_IN]-(:Customer)-[:MAKES]->(:Order)-[:CONTAINS]->(:LineItem)
    (:LineItem)-[:SUPPLIED_BY]->(:Supplier)-[:SUPPLIER_IN]->(:Nation)
    (:Supplier)-[:SUPPLIES {supplycost}]->(:Part)

Only the attributes that the four queries use are kept, as in the MongoDB lab,
so that both labs store the same data. Foreign keys are not stored as
properties: in the graph they become relationships.

Usage:
    python transform_data.py --input-dir ../tpc-h --output-dir transformed-data
"""
import argparse
import csv
import gzip
import sys
from pathlib import Path

# Column names of each TPC-H table, in the order they appear in the .tbl files.
COLUMNS = {
    "region":   ["regionkey", "name", "comment"],
    "nation":   ["nationkey", "name", "regionkey", "comment"],
    "supplier": ["suppkey", "name", "address", "nationkey", "phone", "acctbal",
                 "comment"],
    "customer": ["custkey", "name", "address", "nationkey", "phone", "acctbal",
                 "mktsegment", "comment"],
    "part":     ["partkey", "name", "mfgr", "brand", "type", "size", "container",
                 "retailprice", "comment"],
    "partsupp": ["partkey", "suppkey", "availqty", "supplycost", "comment"],
    "orders":   ["orderkey", "custkey", "orderstatus", "totalprice", "orderdate",
                 "orderpriority", "clerk", "shippriority", "comment"],
    "lineitem": ["orderkey", "partkey", "suppkey", "linenumber", "quantity",
                 "extendedprice", "discount", "tax", "returnflag", "linestatus",
                 "shipdate", "commitdate", "receiptdate", "shipinstruct",
                 "shipmode", "comment"],
}


def line_id(row):
    """Unique id of a line item: order 1000, line 3 -> 10003 (lines go 1 to 7)."""
    return int(row["orderkey"]) * 10 + int(row["linenumber"])


# Nodes: label -> (source table, file name, {csv column: how to compute it}).
# The "id" column is the unique key used to create the relationships.
NODES = {
    "Region": ("region", "regions.csv", {
        "id": lambda r: r["regionkey"], "name": lambda r: r["name"]}),
    "Nation": ("nation", "nations.csv", {
        "id": lambda r: r["nationkey"], "name": lambda r: r["name"]}),
    "Supplier": ("supplier", "suppliers.csv", {
        "id": lambda r: r["suppkey"], "name": lambda r: r["name"],
        "address": lambda r: r["address"], "phone": lambda r: r["phone"],
        "acctbal": lambda r: r["acctbal"], "comment": lambda r: r["comment"]}),
    "Customer": ("customer", "customers.csv", {
        "id": lambda r: r["custkey"], "mktsegment": lambda r: r["mktsegment"]}),
    "Part": ("part", "parts.csv", {
        "id": lambda r: r["partkey"], "mfgr": lambda r: r["mfgr"],
        "type": lambda r: r["type"], "size": lambda r: r["size"]}),
    "Order": ("orders", "orders.csv", {
        "id": lambda r: r["orderkey"], "orderdate": lambda r: r["orderdate"],
        "shippriority": lambda r: r["shippriority"]}),
    "LineItem": ("lineitem", "lineitems.csv", {
        "id": line_id, "linenumber": lambda r: r["linenumber"],
        "quantity": lambda r: r["quantity"],
        "extendedprice": lambda r: r["extendedprice"],
        "discount": lambda r: r["discount"], "tax": lambda r: r["tax"],
        "returnflag": lambda r: r["returnflag"],
        "linestatus": lambda r: r["linestatus"],
        "shipdate": lambda r: r["shipdate"]}),
}

# Relationships: type -> (source table, file, start id, end id, {properties}).
RELATIONSHIPS = {
    "IN_REGION": ("nation", "nation_in_region.csv",
                  lambda r: r["nationkey"], lambda r: r["regionkey"], {}),
    "CUSTOMER_IN": ("customer", "customer_in_nation.csv",
                    lambda r: r["custkey"], lambda r: r["nationkey"], {}),
    "SUPPLIER_IN": ("supplier", "supplier_in_nation.csv",
                    lambda r: r["suppkey"], lambda r: r["nationkey"], {}),
    "MAKES": ("orders", "customer_makes_order.csv",
              lambda r: r["custkey"], lambda r: r["orderkey"], {}),
    "CONTAINS": ("lineitem", "order_contains_lineitem.csv",
                 lambda r: r["orderkey"], line_id, {}),
    "SUPPLIED_BY": ("lineitem", "lineitem_supplied_by_supplier.csv",
                    line_id, lambda r: r["suppkey"], {}),
    "SUPPLIES": ("partsupp", "supplier_supplies_part.csv",
                 lambda r: r["suppkey"], lambda r: r["partkey"],
                 {"supplycost": lambda r: r["supplycost"]}),
}


def read_table(input_dir, table):
    """Yield one dictionary per row of a .tbl or .tbl.gz file."""
    path = input_dir / f"{table}.tbl"
    gz_path = input_dir / f"{table}.tbl.gz"
    if gz_path.exists():
        source = gzip.open(gz_path, "rt", encoding="utf-8", newline="")
    elif path.exists():
        source = path.open("r", encoding="utf-8", newline="")
    else:
        raise FileNotFoundError(f"Input file not found: {gz_path} or {path}")
    columns = COLUMNS[table]
    with source:
        for line_number, row in enumerate(csv.reader(source, delimiter="|"), 1):
            if row and row[-1] == "":        # every line ends with '|'
                row = row[:-1]
            if len(row) != len(columns):
                raise ValueError(f"{table}, line {line_number}: expected "
                                 f"{len(columns)} fields, found {len(row)}")
            yield dict(zip(columns, row))


def write_csv(path, header, rows):
    """Write the rows to a CSV file and return how many there were."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Transform TPC-H tables into Neo4j import CSVs.")
    parser.add_argument("--input-dir", type=Path, default=Path("../tpc-h"),
                        help="folder with the .tbl or .tbl.gz files")
    parser.add_argument("--output-dir", type=Path,
                        default=Path("transformed-data"),
                        help="output folder (default: ./transformed-data)")
    args = parser.parse_args()
    input_dir = args.input_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()

    print("Nodes:")
    for label, (table, file_name, fields) in NODES.items():
        rows = ([compute(r) for compute in fields.values()]
                for r in read_table(input_dir, table))
        count = write_csv(output_dir / "nodes" / file_name, list(fields), rows)
        print(f"  {label:10s} {count:>10,}  -> nodes/{file_name}")

    print("Relationships:")
    for rel_type, (table, file_name, start, end, props) in RELATIONSHIPS.items():
        header = ["start_id", "end_id", *props]
        rows = ([start(r), end(r), *(compute(r) for compute in props.values())]
                for r in read_table(input_dir, table))
        count = write_csv(output_dir / "relationships" / file_name, header, rows)
        print(f"  {rel_type:12s} {count:>10,}  -> relationships/{file_name}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
