package me.vkutuev.inserter

import me.vkutuev.Options
import java.io.File
import java.util.*

class BasicInserter(options: Options) : AbstractInserter(options) {
    override fun initialize() {
        setNeo4jConfig()
        Runtime.getRuntime().exec("${options.neo4jRootPath}/bin/neo4j-admin set-initial-password $password")
                .waitFor()
        runNeo4j()
    }

    override fun insert(testPath: String) {
        Scanner(File(testPath + File.separator + "nodes.csv")).use { scanner ->
            session!!.beginTransaction().use { tx ->
                while (scanner.hasNextInt()) {
                    val nodeId = scanner.nextInt()
                    try {
                        tx.run("MERGE (:$defaultNodeLabelName{$idField:$nodeId})")
                    } catch (e: Exception) {
                        tx.failure()
                        throw e
                    }
                }
                tx.success()
            }
        }

        Scanner(File(testPath + File.separator + "edges.csv")).use { scanner ->
            session!!.writeTransaction { tx ->
                while (scanner.hasNext()) {
                    val line = scanner.next()
                    if (line.isNullOrBlank() or !line.contains(','))
                        break
                    val fromId = line.substringBefore(',')
                    val toId = line.substringAfter(',')
                    try {
                        tx.run("""MATCH (from{$idField:$fromId}), (to{$idField:$toId}) MERGE (from)-[:$relationshipType]->(to)""")
                    } catch (e: Exception) {
                        tx.failure()
                        throw e
                    }
                }
                tx.success()
            }
        }
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