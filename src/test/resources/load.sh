#until nodetool status; do
#    sleep 5
#    echo "Waiting for Cassandra..." 
#done
sleep 5
cqlsh -f src/test/resources/load.cql
