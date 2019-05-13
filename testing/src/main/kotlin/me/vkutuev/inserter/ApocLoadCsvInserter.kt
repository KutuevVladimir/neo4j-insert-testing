package me.vkutuev.inserter

import me.vkutuev.Options
import java.io.File
import java.io.FileReader
import java.io.FileWriter

class ApocLoadCsvInserter(options: Options) : AbstractInserter(options) {

    override fun initialize() {
        setNeo4jConfig()
        var confText = File(options.neo4jConfigPath).readText()
        if (Regex("""^apoc\.import\.file\.enabled=\w+$""", RegexOption.MULTILINE).containsMatchIn(confText)) {
            confText = Regex("""^apoc\.import\.file\.enabled=\w+$""", RegexOption.MULTILINE).replace(
                    confText,
                    "apoc.import.file.enabled=true"
            )
            File(options.neo4jConfigPath).writeText(confText)
        } else {
            File(options.neo4jConfigPath).writeText("$confText\napoc.import.file.enabled=true")
        }
        Runtime.getRuntime().exec("${options.neo4jRootPath}/bin/neo4j-admin set-initial-password $password")
                .waitFor()
        runNeo4j()
    }

    override fun insert(testPath: String) {
        val timestamp = System.nanoTime()
        val nodesCsv = File("nodes-$timestamp.csv")
        FileWriter(nodesCsv).use { writer ->
            writer.write("nodeId\n")
            writer.flush()
            File(testPath + File.separator + "nodes.csv").forEachLine { writer.write("$it\n") }
        }

        val edgesCsv = File("edges-$timestamp.csv")
        FileWriter(edgesCsv).use { writer ->
            writer.write("fromId,toId\n")
            File(testPath + File.separator + "edges.csv").forEachLine { writer.write("$it\n") }
            writer.flush()
        }

        session!!.beginTransaction().use { tx ->
            try {
                tx.run("""CALL apoc.periodic.iterate(
                    "CALL apoc.load.csv('FILE://${nodesCsv.absolutePath}') yield map as row return row",
                    "MERGE (p:$defaultNodeLabelName {$idField:row.nodeId})",
                    {batchSize:10000, iterateList:true, parallel:true}
                    );""")
                tx.run("""CALL apoc.periodic.iterate(
                    "CALL apoc.load.csv('FILE://${edgesCsv.absolutePath}') yield map as row return row",
                    "MATCH (f:$defaultNodeLabelName {$idField:row.fromId})
                        OPTIONAL MATCH (t:$defaultNodeLabelName {$idField:row.toId})
                        MERGE (f)-[:$relationshipType]->(t)",
                    {batchSize:10000, iterateList:true, parallel:true}
                    );""")
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

    override fun finalize() {
        stopNeo4j()
        cleanData()
    }

    override fun reset() {
        finalize()
        initialize()
    }

}