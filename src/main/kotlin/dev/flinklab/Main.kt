package dev.flinklab

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress

private val mapper = ObjectMapper()

private fun HttpExchange.send(code: Int, body: ByteArray, contentType: String) {
    responseHeaders.add("Content-Type", contentType)
    sendResponseHeaders(code, if (body.isEmpty()) -1 else body.size.toLong())
    if (body.isNotEmpty()) responseBody.use { it.write(body) }
    close()
}

private fun HttpExchange.json(code: Int, value: Any?) = send(code, mapper.writeValueAsBytes(value), "application/json")

private fun HttpExchange.fail(code: Int, message: String) = json(code, mapOf("error" to message))

fun scenarioMap(s: Scenario) = mapOf(
    "id" to s.id,
    "title" to s.title,
    "summary" to s.summary,
    "concepts" to s.concepts,
    "inputHint" to s.inputHint,
    "defaultInput" to s.defaultInput,
    "eventTime" to s.eventTime,
    "defaultParallelism" to s.defaultParallelism,
)

/** Validates a submission body; throws IllegalArgumentException with a user-facing message. */
fun parseRequest(body: Map<*, *>): JobRequest {
    val scenario = Scenarios.find(body["scenario"]?.toString() ?: "")
        ?: throw IllegalArgumentException("Unknown scenario")
    val input = body["input"]?.toString() ?: scenario.defaultInput
    if (input.isBlank()) throw IllegalArgumentException("Input must not be empty")
    if (input.length > 100_000) throw IllegalArgumentException("Input too large")
    val parallelism = (body["parallelism"] as? Number)?.toInt() ?: scenario.defaultParallelism
    if (parallelism !in 1..8) throw IllegalArgumentException("Parallelism must be between 1 and 8")
    val delay = (body["watermarkDelaySec"] as? Number)?.toLong() ?: if (scenario.id == "late-data") 0L else 3L
    if (delay !in 0..3600) throw IllegalArgumentException("Watermark delay must be between 0 and 3600")
    return JobRequest(scenario, input, parallelism, body["mode"] == "BATCH", delay)
}

fun startServer(port: Int, manager: JobManager): HttpServer {
    val server = HttpServer.create(InetSocketAddress(System.getenv("HOST") ?: "127.0.0.1", port), 0)
    server.createContext("/") { ex ->
        try {
            val path = ex.requestURI.path
            val method = ex.requestMethod
            when {
                method == "GET" && (path == "/" || path == "/index.html") -> {
                    val html = object {}.javaClass.getResourceAsStream("/static/index.html")!!.readBytes()
                    ex.send(200, html, "text/html; charset=utf-8")
                }
                method == "GET" && path == "/api/scenarios" -> ex.json(200, Scenarios.all.map(::scenarioMap))
                method == "GET" && path == "/api/jobs" -> ex.json(200, manager.list().map { it.toMap(false) })
                method == "POST" && path == "/api/jobs" -> {
                    val body = try {
                        mapper.readValue(ex.requestBody.readNBytes(200_000), Map::class.java)
                    } catch (e: Exception) {
                        return@createContext ex.fail(400, "Invalid JSON")
                    }
                    try {
                        ex.json(201, manager.submit(parseRequest(body)).toMap(false))
                    } catch (e: IllegalArgumentException) {
                        ex.fail(400, e.message ?: "Bad request")
                    }
                }
                method == "GET" && path.startsWith("/api/jobs/") -> {
                    val job = path.removePrefix("/api/jobs/").toIntOrNull()?.let(manager::get)
                    if (job == null) ex.fail(404, "No such job") else ex.json(200, job.toMap(true))
                }
                else -> ex.fail(404, "Not found")
            }
        } catch (e: Throwable) {
            runCatching { ex.fail(500, e.message ?: "Internal error") }
        }
    }
    server.start()
    return server
}

fun main(args: Array<String>) {
    val port = args.firstOrNull()?.toIntOrNull() ?: System.getenv("PORT")?.toIntOrNull() ?: 8080
    val manager = JobManager()
    startServer(port, manager)
    println("Flink Kotlyn Lab running at http://localhost:$port")
}
