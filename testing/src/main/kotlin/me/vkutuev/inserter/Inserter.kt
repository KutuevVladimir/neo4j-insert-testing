package me.vkutuev.inserter

interface Inserter {
    fun initialize()
    fun insert(testPath: String)
    fun runQuery(statement: String): String
    fun reset()
    fun finalize()
}