#!/bin/bash

# Start 7 network nodes, then allocate the resources 4 times
java DatabaseNode -tcpport 9000 -record 1:8 &>/dev/null &
sleep 1
java DatabaseNode -tcpport 9001 -connect localhost:9000 -record 2:7 &>/dev/null &
sleep 1
java DatabaseNode -tcpport 9002 -connect localhost:9001 -record 3:6 &>/dev/null &
sleep 1
java DatabaseNode -tcpport 9003 -connect localhost:9002 -record 4:5 &>/dev/null &
sleep 1
java DatabaseNode -tcpport 9004 -connect localhost:9003 -record 5:4 &>/dev/null &
sleep 1
java DatabaseNode -tcpport 9005 -connect localhost:9004 -record 6:3 &>/dev/null &
sleep 1
java DatabaseNode -tcpport 9006 -connect localhost:9005 -connect localhost:9000 -record 7:1 &>/dev/null &
sleep 1

export verbose=false

java DatabaseClient -gateway localhost:9003 -operation get-max
java DatabaseClient -gateway localhost:9004 -operation get-min
sleep 1
java DatabaseClient -gateway localhost:9004 -operation find-key 7
java DatabaseClient -gateway localhost:9003 -operation find-key 1
sleep 1
java DatabaseClient -gateway localhost:9003 -operation set-value 7:7
java DatabaseClient -gateway localhost:9004 -operation set-value 1:1
sleep 1
java DatabaseClient -gateway localhost:9004 -operation get-max
java DatabaseClient -gateway localhost:9003 -operation get-min
sleep 1
java DatabaseClient -gateway localhost:9006 -operation find-key 7
java DatabaseClient -gateway localhost:9006 -operation get-value 7
java DatabaseClient -gateway localhost:9000 -operation find-key 1
java DatabaseClient -gateway localhost:9000 -operation get-value 1
sleep 1
java DatabaseClient -gateway localhost:9004 -operation new-record 42:42
java DatabaseClient -gateway localhost:9006 -operation find-key 42
java DatabaseClient -gateway localhost:9006 -operation get-value 42
sleep 1

java DatabaseClient -gateway localhost:9000 -operation terminate
java DatabaseClient -gateway localhost:9001 -operation terminate
java DatabaseClient -gateway localhost:9002 -operation terminate
java DatabaseClient -gateway localhost:9003 -operation terminate
java DatabaseClient -gateway localhost:9004 -operation terminate
java DatabaseClient -gateway localhost:9005 -operation terminate
java DatabaseClient -gateway localhost:9006 -operation terminate
