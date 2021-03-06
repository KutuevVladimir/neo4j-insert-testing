package me.vkutuev.inserter

import me.vkutuev.Options
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import java.io.File
import java.util.*

class EmbeddedDatabaseInserter(options: Options) : AbstractInserter(options) {

    private lateinit var database: GraphDatabaseService

    override fun initialize() {
        database =
                GraphDatabaseFactory().newEmbeddedDatabase(File("${options.dataPath}/${options.databaseName}"))
    }

    override fun insert(testPath: String) {
        Scanner(File(testPath + File.separator + "nodes.csv")).use { scanner ->
            database.beginTx().use { tx ->
                while (scanner  .hasNextInt()) {
                    val nodeId = scanner.nextInt()
                    val dbNode = database.findNode({ defaultNodeLabelName }, idField, nodeId)
                            ?: database.createNode(Label { defaultNodeLabelName })
                    val nodeLock = tx.acquireWriteLock(dbNode)
                    dbNode.setProperty(idField, nodeId)
                    nodeLock.release()
                }
                tx.success()
            }
        }
        Scanner(File(testPath + File.separator + "edges.csv")).use { scanner ->
            database.beginTx().use { tx ->
                while (scanner.hasNext()) {
                    val line = scanner.next()
                    if (line.isNullOrBlank() or !line.contains(','))
                        break
                    val fromId = line.substringBefore(',').toLong()
                    val toId = line.substringAfter(',').toLong()
                    val fromNode = database.findNode({ defaultNodeLabelName }, idField, fromId)
                    fromNode ?: nodeNotFound(fromId, tx)
                    val toNode = database.findNode({ defaultNodeLabelName }, idField, toId)
                    toNode ?: nodeNotFound(toId, tx)
                    val fromNodeLock = tx.acquireWriteLock(fromNode)
                    val toNodeLock = tx.acquireWriteLock(toNode)
                    fromNode.createRelationshipTo(toNode) { relationshipType }
                    fromNodeLock.release()
                    toNodeLock.release()
                }
                tx.success()
            }
        }
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

    private fun nodeNotFound(id: Long, tx: Transaction) {
        tx.failure()
        throw Exception("Can't find node $id which is necessary to create relationship")
    }
}