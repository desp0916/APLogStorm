#!/bin/sh

export TOPOLOGY_JAR="target/aplogstorm-0.0.1-SNAPSHOT.jar"
export STORM_CMD="/bin/storm"	# HDP2.6.0
export TOPOLOGY_PACKAGE="com.pic.ala.storm"
export TOPOLOGIES="ApLogGenerator ApLogAnalyzer LogAnalyzer"

for topology in $TOPOLOGIES
do
    $STORM_CMD jar $TOPOLOGY_JAR ${TOPOLOGY_PACKAGE}.${topology}
done

