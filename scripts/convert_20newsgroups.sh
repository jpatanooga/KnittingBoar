#!/bin/bash

JAVA_HOME="/usr/lib/jvm/java-1.6.0"
JAVA_OPTS=""
JAR=" com.cloudera.knittingboar.conf.cmdline.DataConverterCmdLineDriver"

execJAR="../target/KnittingBoar-1.0-SNAPSHOT-jar-with-dependencies.jar"

JAVA_CMD="/usr/bin/java"



export HADOOP_NICENESS=0

cmd=$1

$JAVA_CMD -cp $execJAR:$(echo lib/*.jar | tr ' ' ':') ${JAR} $@ 

#echo ../target/lib/*.jar | tr ' ' ':' 
