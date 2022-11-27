#!/bin/bash

# Start 1 network node, then change and find a value using it
java DatabaseNode -tcpport 9000 -record 1:1 &
sleep 1
java DatabaseClient -gateway localhost:9000 -operation set-value 1:2
java DatabaseClient -gateway localhost:9000 -operation get-value 2
java DatabaseClient -gateway localhost:9000 -operation terminate
