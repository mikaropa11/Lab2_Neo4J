"""
The four TPC-H queries in Cypher, as written by the team, and the same
queries in SQL (Appendix B of the statement) to check the results with DuckDB.

The values of the statement are passed as parameters ($segment, $date...),
so the text of each query never changes.
"""

PARAMS = {
    "q1": {"date": "1998-09-02"},                       # 1998-12-01 - 90 days
    "q2": {"size": 15, "type": "BRASS", "region": "EUROPE"},
    "q3": {"segment": "BUILDING", "date": "1995-03-15"},
    "q4": {"region": "ASIA", "date": "1994-01-01"},
}

CYPHER = {
    "q1": """
        MATCH (l:LineItem)
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

    "q2": """
        MATCH (p:Part)<-[sc:SUPPLIES]-(s:Supplier)-[:SUPPLIER_IN]->(n:Nation)-[:IN_REGION]->(r:Region)
        WHERE p.size = $size AND p.type ENDS WITH $type AND r.name = $region
        WITH p, min(sc.supplycost) AS min_cost
        MATCH (p)<-[sc2:SUPPLIES]-(s2:Supplier)-[:SUPPLIER_IN]->(n2:Nation)-[:IN_REGION]->(r2:Region)
        WHERE r2.name = $region AND sc2.supplycost = min_cost
        RETURN s2.acctbal, s2.name, n2.name, p.id, p.mfgr,
               s2.address, s2.phone, s2.comment
        ORDER BY s2.acctbal DESC, n2.name, s2.name, p.id""",

    "q3": """
        MATCH (c:Customer)-[:MAKES]->(o:Order)-[:CONTAINS]->(l:LineItem)
        WHERE c.mktsegment = $segment
          AND o.orderdate < date($date)
          AND l.shipdate > date($date)
        RETURN o.id, sum(l.extendedprice * (1 - l.discount)) AS revenue,
               o.orderdate, o.shippriority
        ORDER BY revenue DESC, o.orderdate""",

    "q4": """
        MATCH (r:Region)<-[:IN_REGION]-(n:Nation)<-[:CUSTOMER_IN]-(c:Customer)
              -[:MAKES]->(o:Order)-[:CONTAINS]->(l:LineItem)
              -[:SUPPLIED_BY]->(s:Supplier)-[:SUPPLIER_IN]->(n)
        WHERE r.name = $region
          AND o.orderdate >= date($date)
          AND o.orderdate < date($date) + duration({years: 1})
        RETURN n.name, sum(l.extendedprice * (1 - l.discount)) AS revenue
        ORDER BY revenue DESC""",
}

SQL = {
    "q1": """select l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice),
                sum(l_extendedprice*(1-l_discount)),
                sum(l_extendedprice*(1-l_discount)*(1+l_tax)),
                avg(l_quantity), avg(l_extendedprice), avg(l_discount), count(*)
             from lineitem where l_shipdate <= date '{date}'
             group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus""",
    "q2": """select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
             from part, supplier, partsupp, nation, region
             where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = {size}
               and p_type like '%{type}' and s_nationkey = n_nationkey
               and n_regionkey = r_regionkey and r_name = '{region}'
               and ps_supplycost = (select min(ps_supplycost)
                   from partsupp, supplier, nation, region
                   where p_partkey = ps_partkey and s_suppkey = ps_suppkey
                     and s_nationkey = n_nationkey and n_regionkey = r_regionkey
                     and r_name = '{region}')
             order by s_acctbal desc, n_name, s_name, p_partkey""",
    "q3": """select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue,
                o_orderdate, o_shippriority
             from customer, orders, lineitem
             where c_mktsegment = '{segment}' and c_custkey = o_custkey
               and l_orderkey = o_orderkey and o_orderdate < date '{date}'
               and l_shipdate > date '{date}'
             group by l_orderkey, o_orderdate, o_shippriority
             order by revenue desc, o_orderdate""",
    "q4": """select n_name, sum(l_extendedprice*(1-l_discount)) as revenue
             from customer, orders, lineitem, supplier, nation, region
             where c_custkey = o_custkey and l_orderkey = o_orderkey
               and l_suppkey = s_suppkey and c_nationkey = s_nationkey
               and s_nationkey = n_nationkey and n_regionkey = r_regionkey
               and r_name = '{region}' and o_orderdate >= date '{date}'
               and o_orderdate < date '{date}' + interval 1 year
             group by n_name order by revenue desc""",
}

# Indexes on the properties that the queries filter on (phase 2 of the benchmark).
INDEXES = {
    "customer_mktsegment": "FOR (n:Customer) ON (n.mktsegment)",
    "order_orderdate": "FOR (n:Order) ON (n.orderdate)",
    "lineitem_shipdate": "FOR (n:LineItem) ON (n.shipdate)",
    "part_size": "FOR (n:Part) ON (n.size)",
    "region_name": "FOR (n:Region) ON (n.name)",
}
