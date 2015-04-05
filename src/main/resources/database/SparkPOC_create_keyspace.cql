CREATE KEYSPACE "spark_poc" WITH REPLICATION = {
    'class' : 'NetworkTopologyStrategy',
    'us-west-2' : '3',
    'us-east' : '3'};