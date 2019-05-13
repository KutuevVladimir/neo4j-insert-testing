package me.vkutuev

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.time.*
import java.util.*

class Neo4jInserter {
    companion object {
        @JvmStatic
        val defaultNodeLabel = Label { "Node" }

        @JvmStatic
        val idField = "id"
    }

    @Context
    @JvmField
    public var db: GraphDatabaseService? = null

    @Context
    @JvmField
    public var log: Log? = null

    @Procedure(name = "vkutuev.insert.nodes", mode = Mode.WRITE)
    @Description("Insert nodes described in csv to database")
    public fun insertNodes(@Name("csv path") filePath: String): Unit {
        BufferedReader(FileReader(File(filePath))).use { br ->
            val headerValues = br.readLine()
                    .split(',')
                    .map { Pair(it.split(':').first(), it.split(':').last()) }
                    .toTypedArray()
            var line = br.readLine()
            while (line != null) {
                val lineValues = line.split(',')
                line = br.readLine()
                val nodeProperties = HashMap<String, Any>()
                var labels: String? = null
                lineValues.iterator().withIndex().forEach { (index, value) ->
                    val (key, type) = headerValues[index]
                    when (type) {
                        "ID" -> nodeProperties[idField] = value.toType("ID")
                        "LABELS" -> labels = value
                        else -> nodeProperties[key] = value.toType(type)
                    }
                }
                val labelsList = labels?.split(";")?.toMutableList() ?: LinkedList()
                labelsList.add(defaultNodeLabel.name())
                db?.let { db ->
                    val node =
                            db.findNode(defaultNodeLabel, idField, nodeProperties[idField]) ?: db.createNode(
                                    defaultNodeLabel
                            )
                    labelsList.forEach { node.addLabel { it } }
                    nodeProperties.forEach { (key, value) -> node.setProperty(key, value) }
                }
            }
        }
    }

    @Procedure(name = "vkutuev.insert.edges", mode = Mode.WRITE)
    @Description("Insert relationships described in csv to database")
    public fun insertEdges(@Name("csv path") filePath: String): Unit {
        BufferedReader(FileReader(File(filePath))).use { br ->
            val headerValues = br.readLine()
                    .split(',')
                    .map { Pair(it.split(':').first(), it.split(':').last()) }
                    .toTypedArray()
            var line = br.readLine()
            while (line != null) {
                val lineValues = line.split(',')
                line = br.readLine()
                val edgeProperties = HashMap<String, Any>()
                var edgeType: String? = null
                var edgeStart: Any? = null
                var edgeEnd: Any? = null
                lineValues.iterator().withIndex().forEach { (index, value) ->
                    val (key, type) = headerValues[index]
                    when (type) {
                        "TYPE" -> edgeType = value
                        "START_ID" -> edgeStart = value.toType("START_ID")
                        "END_ID" -> edgeEnd = value.toType("END_ID")
                        else -> edgeProperties[key] = value.toType(type)
                    }
                }
                db?.let { db ->
                    val startNode = db.findNode(defaultNodeLabel, idField, edgeStart)
                            ?: throw Exception("There is not start node with $idField=$edgeStart")
                    val endNode = db.findNode(defaultNodeLabel, idField, edgeEnd)
                            ?: throw Exception("There is not end node with $idField=$edgeEnd")
                    val relationship = startNode.createRelationshipTo(endNode, { edgeType })
                    edgeProperties.forEach { (key, value) -> relationship.setProperty(key, value) }
                }
            }
        }
    }


    private fun String.toType(type: String): Any = when (type) {
        "STRING" -> this
        "ID", "INTEGER", "START_ID", "END_ID" -> this.toLong()
        "FLOAT" -> this.toDouble()
        "BOOLEAN" -> this.toBoolean()
        "DATE" -> LocalDate.parse(this)
        "TIME" -> OffsetTime.parse(this)
        "LOCALTIME" -> LocalTime.parse(this)
        "DATETIME" -> ZonedDateTime.parse(this)
        "LOCALDATETIME" -> LocalDateTime.parse(this)
        "DURATION" -> Duration.parse(this)
        else -> this
//        else -> throw Exception("There is no type '$type'")
    }

}