until nodetool status; do
    sleep 5
    echo "Waiting for Cassandra..." 
done
cqlsh -f src/test/resources/load.cql
