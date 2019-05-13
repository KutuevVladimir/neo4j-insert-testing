package me.vkutuev.inserter

import me.vkutuev.Options
import java.io.File
import java.io.FileReader
import java.io.FileWriter

class LoadCsvInserter(options: Options) : AbstractInserter(options) {
    override fun initialize() {
        setNeo4jConfig()
        Runtime.getRuntime().exec("${options.neo4jRootPath}/bin/neo4j-admin set-initial-password $password")
                .waitFor()
        runNeo4j()
    }

    override fun insert(testPath: String) {
        val timestamp = System.nanoTime()
        val nodesCsv = File("nodes-$timestamp.csv")
        FileWriter(nodesCsv).use { writer ->
            writer.write("nodeId\n")
            File(testPath + File.separator + "nodes.csv").forEachLine { writer.write("$it\n") }
            writer.flush()
        }

        val edgesCsv = File("edges-$timestamp.csv")
        FileWriter(edgesCsv).use { writer ->
            writer.write("fromId,toId\n")
            File(testPath + File.separator + "edges.csv").forEachLine { writer.write("$it\n") }
            writer.flush()
        }

        session!!.beginTransaction().use { tx ->
            try {
                tx.run("""LOAD CSV WITH HEADERS FROM 'FILE://${nodesCsv.absolutePath}' AS line
                    MERGE (:$defaultNodeLabelName {$idField:line.nodeId})""")
                tx.run("""LOAD CSV WITH HEADERS FROM 'FILE://${edgesCsv.absolutePath}' AS line
                    MATCH (from:$defaultNodeLabelName {$idField:line.fromId})
                    OPTIONAL MATCH (to:$defaultNodeLabelName {$idField:line.toId})
                    MERGE (from)-[:$relationshipType]->(to)""")
                tx.success()
            } catch (e: Exception) {
                tx.failure()
            }
        }
        nodesCsv.delete()
        edgesCsv.delete()
    }

    override fun runQuery(statement: String): String {
        return getServerResponse(session!!, statement)
    }

    override fun reset() {
        finalize()
        initialize()
    }

    override fun finalize() {
        stopNeo4j()
        cleanData()
    }

}