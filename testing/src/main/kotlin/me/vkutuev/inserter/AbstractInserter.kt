package me.vkutuev.inserter

import me.vkutuev.Options
import org.neo4j.driver.v1.*
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.impl.core.NodeProxy
import org.neo4j.kernel.impl.core.RelationshipProxy
import java.io.File
import java.lang.StringBuilder

abstract class AbstractInserter(protected val options: Options) : Inserter {

    companion object {
        const val defaultNodeLabelName = "Node"
        const val password = "belka"
        const val idField = "id"
        const val relationshipType = "KNOWS"
    }

    protected var driver: Driver? = null
    protected var session: Session? = null

    protected fun getServerResponse(session: Session, statement: String): String {
        val response = StringBuilder()
        session.beginTransaction().use { tx ->
            try {
                val result = tx.run(statement)
                result.forEach { rec ->
                    rec.fields().forEach { pair ->
                        val fieldName = pair.key()
                        val fieldValue = pair.value()
//                        println(fieldValue.type().name())
                        when (fieldValue.type().name()) {
                            "NODE" -> {
                                val node = fieldValue.asNode()
                                response.append("$fieldName:(${node.labels()}{")
                                response.append(node.keys().joinToString(", ") { key -> "$key:${node[key]}" })
                                response.append("})\n")
                            }
                            "RELATIONSHIP" -> {
                                val edge = fieldValue.asRelationship()
                                response.append("$fieldName:(${edge.startNodeId()})-[${edge.type()}{")
                                response.append(edge.keys().joinToString(", ") { key -> "$key:${edge[key]}" })
                                response.append("}]->(${edge.endNodeId()})\n")
                            }
                            else -> response.append("$fieldName:${strFromFieldValue(fieldValue)}\n")
                        }
                    }
                }
                tx.success()
            } catch (e: Exception) {
                tx.failure()
                throw e
            }
        }
        return response.toString()
    }

    protected fun getEmbeddedResponse(database: GraphDatabaseService, statement: String): String {
        val result = database.execute(statement)
        val response = StringBuilder()
        result.forEach { row ->
            result.columns().forEach { key ->
                val fieldValue = row[key]
                when (fieldValue) {
                    null -> response.append("$key:NULL\n")
                    is NodeProxy -> {
                        database.beginTx().use { tx ->
                            val node = database.getNodeById(fieldValue.id)

                            response.append("$key:([")
                            response.append(node.labels.joinToString(", "))
                            response.append("]{")
                            response.append(
                                    node.allProperties
                                            .map { (key, value) -> if (value is String) "$key:\"$value\"" else "$key:$value" }
                                            .joinToString(", ")
                            )
                            response.append("})\n")
                            tx.success()
                        }
                    }
                    is RelationshipProxy -> {
                        database.beginTx().use { tx ->
                            val relationship = database.getRelationshipById(fieldValue.id)
                            response.append("$key:(${fieldValue.startNodeId})-[")
                            response.append(relationship.type.name())
                            response.append("{")
                            response.append(
                                    relationship.allProperties
                                            .map { (key, value) -> "$key:$value" }
                                            .joinToString(", ")
                            )
                            response.append("}")
                            response.append("]-(${relationship.endNodeId})\n")
                            tx.success()
                        }
                    }
                    else -> response.append("$key:$fieldValue\n")
                }
            }
        }
        return response.toString()
    }

    private fun strFromFieldValue(fieldValue: Value): Any = when (fieldValue.type().name()) {
        "STRING", "NULL" -> fieldValue
        "ID", "INTEGER" -> fieldValue.asLong()
        "FLOAT" -> fieldValue.asDouble()
        "BOOLEAN" -> fieldValue.asBoolean()
        "DATE" -> fieldValue.asLocalDate()
        "TIME" -> fieldValue.asOffsetTime()
        "LOCALTIME" -> fieldValue.asLocalTime()
        "DATETIME" -> fieldValue.asZonedDateTime()
        "LOCALDATETIME" -> fieldValue.asLocalDateTime()
        "DURATION" -> fieldValue.asIsoDuration()
        else -> throw Exception("There is no type ${fieldValue.type().name()}")
//        else -> {
//            print(fieldValue.type().name())
//        }
    }

    private fun removeDir(dir: File): Boolean {
        if (dir.isDirectory) {
            if (!dir.list().fold(true) { acc, child -> acc and removeDir(File(dir, child)) }) {
                throw Exception("Can't remove directory '${dir.absoluteFile}'")
            }
        }
        return dir.delete()
    }

    protected fun runNeo4j() {
        Runtime.getRuntime().exec("${options.neo4jCommand} start").waitFor()
        driver = null
        while (driver == null) {
            try {
                driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", password))
            } catch (e: ServiceUnavailableException) {
                // Do nothing, because it's a normal situation (database isn't ready yet)
            } catch (e: Exception) {
                print(e.message)
            }
        }
        session = driver!!.session()
    }

    protected fun stopNeo4j() {
        Runtime.getRuntime().exec("${options.neo4jCommand} stop").waitFor()
        session?.close()
        driver?.close()
        session = null
        driver = null
    }

    protected fun cleanData() {
        removeDir(File("${options.dataPath}/${options.databaseName}"))
    }

    protected fun setNeo4jConfig() {
        with(options) {
            if (neo4jOwnConfigs == "y") {
                var conf = File(neo4jConfigPath).readText()
                conf = kotlin.text.Regex("""^dbms\.active_database=\w+\.db""", RegexOption.MULTILINE).replace(
                        conf,
                        "dbms.active_database=$databaseName"
                )
                conf = kotlin.text.Regex("""^#?dbms\.directories\.data=.+$""", RegexOption.MULTILINE).replace(
                        conf,
                        "dbms.directories.data=$dataPath"
                )
                conf = kotlin.text.Regex("""^ *dbms\.directories\.import=.+$""", RegexOption.MULTILINE).replace(
                        conf,
                        "#dbms.directories.import=import"
                )
                File(neo4jConfigPath).writeText(conf)
            }
        }
    }
}