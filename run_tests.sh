#!/usr/bin/env bash

DIR=$(dirname $0)
TEST_PATH=$1
TESTING_CP="${DIR}/testing/libs/*:${DIR}/testing/build/libs/*"
echo "${TESTING_CP}"
#${DIR}/gradlew :testing:clean :testing:copyDependencies :testing:jar
runTest() {
    java -cp "${TESTING_CP}" me.vkutuev.MainKt --Neo4jCommand=/var/lib/neo4j/bin/neo4j --TestPath=${test} --CyclesCount=5 --InsertMethod=$1
    /var/lib/neo4j/bin/neo4j stop
    rm -rf /data/databases/graph.db/
}
for test in ${TEST_PATH}/*
do
    echo ${test}
    runTest Basic
    runTest LoadCsv
    runTest ApocLoadCsv
    runTest UserProcedures
    runTest BatchNeo4j
    runTest Embedded
    runTest BatchEmbedded
done
