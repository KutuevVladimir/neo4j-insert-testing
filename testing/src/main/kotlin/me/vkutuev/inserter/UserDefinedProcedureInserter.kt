package me.vkutuev.inserter

import me.vkutuev.Options
import java.io.File
import java.io.FileReader
import java.io.FileWriter

class UserDefinedProcedureInserter(options: Options) : AbstractInserter(options) {
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
            writer.write(":ID\n")
            FileReader(testPath + File.separator + "nodes.csv").use { reader ->
                reader.forEachLine { nodeLine ->
                    writer.write("$nodeLine\n")
                }
            }
            writer.flush()
        }

        val edgesCsv = File("edges-$timestamp.csv")
        FileWriter(edgesCsv).use { writer ->
            writer.write(":START_ID,:END_ID,:TYPE\n")
            FileReader(testPath + File.separator + "edges.csv").use { reader ->
                reader.forEachLine { edge ->
                    writer.write("$edge,$relationshipType\n")
                }
            }
            writer.flush()
        }

        session!!.beginTransaction().use { tx ->
            try {
                tx.run("CALL vkutuev.insert.nodes('${nodesCsv.absolutePath}')")
                tx.run("CALL vkutuev.insert.edges('${edgesCsv.absolutePath}')")
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