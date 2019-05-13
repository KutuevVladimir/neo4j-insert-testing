package me.vkutuev.inserter

import me.vkutuev.Options
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.unsafe.batchinsert.BatchInserter
import org.neo4j.unsafe.batchinsert.BatchInserters
import java.io.File
import java.util.*

class BatchEmbeddedInserter(options: Options) : AbstractInserter(options) {

    private lateinit var database: GraphDatabaseService

    override fun initialize() {
        database =
                GraphDatabaseFactory().newEmbeddedDatabase(File("${options.dataPath}/${options.databaseName}"))
    }

    override fun insert(testPath: String) {
        val databasePath = "${options.dataPath}/${options.databaseName}"
        lateinit var inserter: BatchInserter

        val nodesToAdd = LinkedList<Int>()
        Scanner(File(testPath + File.separator + "nodes.csv")).use { scanner ->
            database.beginTx().use { tx ->
                while (scanner  .hasNextInt()) {
                    val nodeId = scanner.nextInt()
                    val dbNode = database.findNode({ defaultNodeLabelName }, idField, nodeId)
                    if (dbNode == null) {
                        nodesToAdd.add(nodeId)
                    }
                }
                tx.success()
            }
        }
        stop()

        try {
            inserter = BatchInserters.inserter(File(databasePath))
            nodesToAdd.forEach { nodeId ->
                val labels = Array(1) { Label { defaultNodeLabelName } }
                val properties = HashMap<String, Any>()
                properties[idField] = nodeId
                try {
                    inserter.createNode(properties, *labels)
                } catch (e: IllegalArgumentException) {
                    println("Node $nodeId is already used")
                } catch (e: Exception) {
                    throw e
                }
            }
        } finally {
            inserter.shutdown()
        }
        val edges = ArrayList<Pair<Long, Long>>()
        run()

        Scanner(File(testPath + File.separator + "edges.csv")).use { scanner ->
            database.beginTx().use { tx ->
                while (scanner.hasNext()) {
                    val line = scanner.next()
                    if (line.isNullOrBlank() or !line.contains(','))
                        break
                    val fromId = line.substringBefore(',').toLong()
                    val toId = line.substringAfter(',').toLong()
                    edges.add(Pair(database.findNode({ defaultNodeLabelName }, idField, fromId).id,
                            database.findNode({ defaultNodeLabelName }, idField, toId).id))
                }
                tx.success()
            }
        }

        stop()

        try {
            inserter = BatchInserters.inserter(File(databasePath))
            edges.forEach { edge ->
                inserter.createRelationship(
                        edge.first,
                        edge.second,
                        { relationshipType },
                        HashMap<String, Any>()
                )
            }
        } catch (e: Exception) {
            println(e.message)
        } finally {
            inserter.shutdown()
        }
        run()
    }

    override fun runQuery(statement: String): String {
        return getEmbeddedResponse(database, statement)
    }

    override fun reset() {
        finalize()
        initialize()
    }

    override fun finalize() {
        database.shutdown()
        cleanData()
    }

    private fun run() {
        database =
                GraphDatabaseFactory().newEmbeddedDatabase(File("${options.dataPath}/${options.databaseName}"))
    }

    private fun stop() {
        database.shutdown()
    }

    private fun nodeNotFound(id: Int, tx: Transaction) {
        tx.failure()
        throw Exception("Can't find node $id which is necessary to create relationship")
    }
}