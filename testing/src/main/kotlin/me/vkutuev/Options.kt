package me.vkutuev

import com.google.devtools.common.options.Option
import com.google.devtools.common.options.OptionsBase

class Options : OptionsBase() {

    @Option(name = "InsertMethod", defaultValue = "Basic")
    lateinit var insertMethod: String

    @Option(name = "CyclesCount", defaultValue = "1")
    lateinit var cyclesCount: String

    @Option(name = "TestPath", defaultValue = "../tests")
    lateinit var testPath: String

    @Option(name = "Neo4jConfigPath", defaultValue = "/var/lib/neo4j/conf/neo4j.conf")
    lateinit var neo4jConfigPath: String

    @Option(name = "Neo4jCommand", defaultValue = "neo4j")
    lateinit var neo4jCommand: String

    @Option(name = "Neo4jRootPath", defaultValue = "/var/lib/neo4j")
    lateinit var neo4jRootPath: String

    @Option(name = "Neo4jDatabasePath", defaultValue = "/data/databases")
    lateinit var dataPath: String

    @Option(name = "Neo4jDatabaseName", defaultValue = "graph.db")
    lateinit var databaseName: String

    @Option(name = "Neo4jOwnConfigs", defaultValue = "n")
    lateinit var neo4jOwnConfigs: String
}