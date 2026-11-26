
import os
import time

from dotenv import load_dotenv
from neo4j import GraphDatabase


load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")



QUERY_1 = """
MATCH (l:LineItem)
WHERE l.L_SHIPDATE <= date('1998-12-01')

RETURN
    l.L_RETURNFLAG AS returnflag,
    l.L_LINESTATUS AS linestatus,
    sum(l.L_QUANTITY) AS sum_qty,
    sum(l.L_EXTENDEDPRICE) AS sum_base_price,
    sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS sum_disc_price,
    sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) * (1 + l.L_TAX))
        AS sum_charge,
    avg(l.L_QUANTITY) AS avg_qty,
    avg(l.L_EXTENDEDPRICE) AS avg_price,
    avg(l.L_DISCOUNT) AS avg_disc,
    count(*) AS count_order

ORDER BY returnflag, linestatus
"""


QUERY_2 = """
MATCH (p:Part)
WHERE p.P_SIZE = 15
  AND p.P_TYPE ENDS WITH 'BRASS'

MATCH (p)-[ps:SUPPLIED_BY]->(s:Supplier)
      -[:SUPPLIER_IN]->(:Nation)
      -[:BELONGS_TO]->(r:Region)

WHERE r.R_NAME = 'EUROPE'

WITH p, min(ps.PS_SUPPLYCOST) AS min_supply_cost

MATCH (p)-[ps:SUPPLIED_BY]->(s:Supplier)
      -[:SUPPLIER_IN]->(n:Nation)
      -[:BELONGS_TO]->(r:Region)

WHERE r.R_NAME = 'EUROPE'
  AND ps.PS_SUPPLYCOST = min_supply_cost

RETURN
    s.S_ACCTBAL AS acctbal,
    s.S_NAME AS supplier_name,
    n.N_NAME AS nation_name,
    p.P_PARTKEY AS partkey,
    p.P_MFGR AS mfgr,
    s.S_ADDRESS AS address,
    s.S_PHONE AS phone,
    s.S_COMMENT AS comment

ORDER BY
    acctbal DESC,
    nation_name,
    supplier_name,
    partkey
"""


QUERY_3 = """
MATCH (c:Customer)
WHERE c.C_MKTSEGMENT = 'BUILDING'

MATCH (c)-[:PLACED]->(o:Order)
WHERE o.O_ORDERDATE < date('1995-03-15')

MATCH (o)-[:CONTAINS]->(l:LineItem)
WHERE l.L_SHIPDATE > date('1995-03-15')

WITH
    l.L_ORDERKEY AS orderkey,
    o,
    sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue

RETURN
    orderkey,
    revenue,
    o.O_ORDERDATE AS orderdate,
    o.O_SHIPPRIORITY AS shippriority

ORDER BY revenue DESC, orderdate
"""


QUERY_4 = """
MATCH (o:Order)
WHERE o.O_ORDERDATE >= date('1995-01-01')
  AND o.O_ORDERDATE <  date('1995-01-01') + duration({years: 1})

MATCH (c:Customer)-[:PLACED]->(o)-[:CONTAINS]->(l:LineItem)-[:SOURCED_FROM]->(s:Supplier)
WHERE c.C_NATIONKEY = s.S_NATIONKEY

MATCH (s)-[:SUPPLIER_IN]->(n:Nation)-[:BELONGS_TO]->(r:Region {R_NAME: 'EUROPE'})

RETURN n.N_NAME AS nation_name,
       sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
ORDER BY revenue DESC

"""

# """
# MATCH (o:Order)
# WHERE o.O_ORDERDATE >= date('1995-01-01')
#   AND o.O_ORDERDATE < date('1995-01-01') + duration({years: 1})

# MATCH (c:Customer)-[:PLACED]->(o)
#       -[:CONTAINS]->(l:LineItem)
#       -[:SOURCED_FROM]->(s:Supplier)

# MATCH (c)-[:CUSTOMER_IN]->(n:Nation)
#       <-[:SUPPLIER_IN]-(s)

# MATCH (n)-[:BELONGS_TO]->(r:Region)

# WHERE r.R_NAME = 'EUROPE'

# WITH
#     n.N_NAME AS nation_name,
#     sum(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue

# RETURN
#     nation_name,
#     revenue

# ORDER BY revenue DESC
# """



def main():

    if not all([NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD]):
        raise ValueError("Missing Neo4j configuration in .env")

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )

    try:
        driver.verify_connectivity()
        print("Connected to Neo4j successfully!")

        with driver.session(database=NEO4J_DATABASE) as session:

            start = time.perf_counter()

            result = session.run(QUERY_4)
            records = list(result)
            summary = result.consume()

            elapsed = time.perf_counter() - start

            print("\n========== RESULTS ==========")
            print(f"Execution time: {elapsed:.6f} seconds")
            print(f"Returned records: {len(records)}")

            for record in records:
                print(dict(record))

            print("\n========== SUMMARY ==========")
            print(f"Database: {summary.database}")
            print(f"Query type: {summary.query_type}")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
