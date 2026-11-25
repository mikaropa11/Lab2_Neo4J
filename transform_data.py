import argparse
import csv
import gzip
import json
import sys
from pathlib import Path

TABLES = {
    "part": {
        "file": "part.tbl.gz",
        "columns": [
            "P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE",
            "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT",
        ],
        "node_file": "parts.csv",
        "id_column": "P_PARTKEY",
        "id_builder": lambda r: r["P_PARTKEY"],
    },
    "partsupp": {
        "file": "partsupp.tbl.gz",
        "columns": [
            "PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY",
            "PS_SUPPLYCOST", "PS_COMMENT",
        ],

        "node_file": None,
        "id_column": None,
        "id_builder": None,
    },
    "supplier": {
        "file": "supplier.tbl.gz",
        "columns": [
            "S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY",
            "S_PHONE", "S_ACCTBAL", "S_COMMENT",
        ],
        "node_file": "suppliers.csv",
        "id_column": "S_SUPPKEY",
        "id_builder": lambda r: r["S_SUPPKEY"],
    },
    "customer": {
        "file": "customer.tbl.gz",
        "columns": [
            "C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY",
            "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT",
        ],
        "node_file": "customers.csv",
        "id_column": "C_CUSTKEY",
        "id_builder": lambda r: r["C_CUSTKEY"],
    },
    "orders": {
        "file": "orders.tbl.gz",
        "columns": [
            "O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE",
            "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK",
            "O_SHIPPRIORITY", "O_COMMENT",
        ],
        "node_file": "orders.csv",
        "id_column": "O_ORDERKEY",
        "id_builder": lambda r: r["O_ORDERKEY"],
    },
    "lineitem": {
        "file": "lineitem.tbl.gz",
        "columns": [
            "L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER",
            "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX",
            "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE",
            "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT",
        ],
        "node_file": "lineitems.csv",
        "id_column": "id",
        "id_builder": lambda r: f'{r["L_ORDERKEY"]}_{r["L_LINENUMBER"]}',
    },
    "nation": {
        "file": "nation.tbl.gz",
        "columns": ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"],
        "node_file": "nations.csv",
        "id_column": "N_NATIONKEY",
        "id_builder": lambda r: r["N_NATIONKEY"],
    },
    "region": {
        "file": "region.tbl.gz",
        "columns": ["R_REGIONKEY", "R_NAME", "R_COMMENT"],
        "node_file": "regions.csv",
        "id_column": "R_REGIONKEY",
        "id_builder": lambda r: r["R_REGIONKEY"],
    },
}

def read_tbl_gz(path: Path, expected_columns: int):
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    with gzip.open(path, "rt", encoding="utf-8", newline="") as source:
        reader = csv.reader(source, delimiter="|")
        for line_number, row in enumerate(reader, start=1):
            if row and row[-1] == "":
                row = row[:-1]
            if len(row) != expected_columns:
                raise ValueError(
                    f"{path.name}, line {line_number}: expected "
                    f"{expected_columns} fields, found {len(row)}"
                )
            yield row

def write_csv(path: Path, headers, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=headers, extrasaction="raise")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count

def table_dicts(input_dir: Path, table_name: str):
    spec = TABLES[table_name]
    for values in read_tbl_gz(input_dir / spec["file"], len(spec["columns"])):
        yield dict(zip(spec["columns"], values))

def node_rows(input_dir: Path, table_name: str):
    spec = TABLES[table_name]
    for record in table_dicts(input_dir, table_name):
        yield {"id": spec["id_builder"](record), **record}

def rel_rows(input_dir: Path, table_name, start_builder, end_builder):
    for record in table_dicts(input_dir, table_name):
        yield {
            "start_id": start_builder(record),
            "end_id": end_builder(record),
        }

def main():
    parser = argparse.ArgumentParser(
        description="Transform compressed TPC-H tables into Neo4j import CSVs."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("../tpc-h"),
        help="Directory containing the eight .tbl.gz files (default: ../tpc-h)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("transformed-data"),
        help="Output directory (default: ./transformed-data)",
    )
    args = parser.parse_args()

    input_dir = args.input_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    nodes_dir = output_dir / "nodes"
    rels_dir = output_dir / "relationships"
    nodes_dir.mkdir(parents=True, exist_ok=True)
    rels_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
