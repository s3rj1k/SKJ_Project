#!/bin/bash

# Start 7 network nodes, then allocate the resources 4 times
java DatabaseNode -tcpport 9000 -record 1:8 &
sleep 1
java DatabaseNode -tcpport 9001 -connect localhost:9000 -record 2:7 &
sleep 1
java DatabaseNode -tcpport 9002 -connect localhost:9001 -record 3:6 &
sleep 1
java DatabaseNode -tcpport 9003 -connect localhost:9002 -record 4:5 &
sleep 1
java DatabaseNode -tcpport 9004 -connect localhost:9003 -connect localhost:9000 -record 5:4 &
sleep 1

java DatabaseClient -gateway localhost:9001 -operation get-max
java DatabaseClient -gateway localhost:9001 -operation get-min
sleep 1
java DatabaseClient -gateway localhost:9002 -operation terminate
sleep 1
java DatabaseClient -gateway localhost:9001 -operation get-max
java DatabaseClient -gateway localhost:9001 -operation get-max
sleep 1

java DatabaseClient -gateway localhost:9000 -operation terminate
java DatabaseClient -gateway localhost:9001 -operation terminate
java DatabaseClient -gateway localhost:9003 -operation terminate
java DatabaseClient -gateway localhost:9004 -operation terminate
