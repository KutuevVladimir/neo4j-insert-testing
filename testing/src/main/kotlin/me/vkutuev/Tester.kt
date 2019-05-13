package me.vkutuev

import com.google.devtools.common.options.OptionsParser
import me.vkutuev.inserter.*
import org.apache.commons.lang3.time.StopWatch
import java.io.File
import java.io.FileWriter

class Tester(args: Array<String>) {

    companion object {
        const val defaultStatement = """MATCH (n) OPTIONAL MATCH (n)-[r]-() RETURN n,r,n.id"""
    }

    private val options: Options

    private val inserter: Inserter

    private val stopWatch = StopWatch()

    private val cyclesCount: Int

    private val testResults = ArrayList<Pair<Long, Long>>()

    init {
        val optionParser = OptionsParser.newOptionsParser(Options::class.java)
        try {
            optionParser.parseAndExitUponError(args)
        } catch (e: Exception) {
            throw e
        }
        options = optionParser.getOptions(Options::class.java) ?:
                throw Exception("There's problem with parsing command line args")

        cyclesCount = options.cyclesCount.toInt()

        inserter = when (options.insertMethod) {
            "Basic" -> BasicInserter(options)
            "LoadCsv" -> LoadCsvInserter(options)
            "ApocLoadCsv" -> ApocLoadCsvInserter(options)
            "UserProcedures" -> UserDefinedProcedureInserter(options)
            "BatchNeo4j" -> BatchNeo4jInserter(options)
            "Embedded" -> EmbeddedDatabaseInserter(options)
            "BatchEmbedded" -> BatchEmbeddedInserter(options)
            else -> throw Exception("Undefined test kind")
        }
        inserter.initialize()
    }

    fun run() {
        for (i in 1..cyclesCount) {
            Thread.sleep(5000)
            stopWatch.start()
            inserter.insert(options.testPath)
            val result = inserter.runQuery(defaultStatement)
            print(result)
            stopWatch.stop()
            testResults.add(Pair(stopWatch.time, stopWatch.nanoTime))

            inserter.reset()
            stopWatch.reset()
        }
        printTestResults()
        inserter.finalize()
    }

    private fun printTestResults() {
        val testResultsFile = File(options.testPath + File.separator + options.insertMethod + ".txt")
        FileWriter(testResultsFile).use { writer ->
            writer.write("${testResults[0].first}\n")
            var time: Long = 0
            for (i in 1 until cyclesCount) {
                time += testResults[i].first
            }
            writer.write("${time/(cyclesCount - 1)}\n")
        }
    }

}