#!/bin/bash
rm -v $PWD/*.class
javac -verbose --release 8 -classpath $PWD *.java
