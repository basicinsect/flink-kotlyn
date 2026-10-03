package dev.flinklab

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.JobStatus
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class JobRequest(
    val scenario: Scenario,
    val input: String,
    val parallelism: Int,
    val batchMode: Boolean,
    val watermarkDelaySec: Long,
)

/** A submitted job and everything observed while it ran. All mutation goes through synchronized methods. */
class JobRecord(val id: Int, val request: JobRequest) {
    @Volatile var status: String = "QUEUED"
    @Volatile var flinkJobId: String? = null
    @Volatile var error: String? = null
    @Volatile var plan: Any? = null
    val createdAt: Instant = Instant.now()
    @Volatile var startedAt: Instant? = null
    @Volatile var finishedAt: Instant? = null
    private val events = mutableListOf<Map<String, Any?>>()
    private val results = mutableListOf<String>()
    @Volatile var resultCount = 0

    @Synchronized fun log(message: String) {
        events += mapOf("time" to Instant.now().toString(), "message" to message)
    }

    @Synchronized fun addResult(r: String) {
        resultCount++
        if (results.size < MAX_RESULTS) results += r
    }

    @Synchronized fun toMap(detail: Boolean): Map<String, Any?> {
        val m = linkedMapOf<String, Any?>(
            "id" to id,
            "scenario" to request.scenario.id,
            "title" to request.scenario.title,
            "status" to status,
            "parallelism" to request.parallelism,
            "mode" to if (request.batchMode) "BATCH" else "STREAMING",
            "flinkJobId" to flinkJobId,
            "createdAt" to createdAt.toString(),
            "startedAt" to startedAt?.toString(),
            "finishedAt" to finishedAt?.toString(),
            "durationMs" to startedAt?.let { java.time.Duration.between(it, finishedAt ?: Instant.now()).toMillis() },
            "resultCount" to resultCount,
            "error" to error,
        )
        if (detail) {
            m["input"] = request.input
            m["watermarkDelaySec"] = request.watermarkDelaySec
            m["plan"] = plan
            m["events"] = events.toList()
            m["results"] = results.toList()
        }
        return m
    }

    companion object {
        const val MAX_RESULTS = 1000
    }
}

class JobManager(workers: Int = 2) {
    private val pool = Executors.newFixedThreadPool(workers) { r -> Thread(r, "job-runner").apply { isDaemon = true } }
    private val ids = AtomicInteger()
    private val jobs = java.util.concurrent.ConcurrentHashMap<Int, JobRecord>()
    private val mapper = ObjectMapper()

    fun submit(request: JobRequest): JobRecord {
        val job = JobRecord(ids.incrementAndGet(), request)
        jobs[job.id] = job
        job.log("Job #${job.id} queued: ${request.scenario.title}")
        pool.submit { run(job) }
        return job
    }

    fun get(id: Int): JobRecord? = jobs[id]

    fun list(): List<JobRecord> = jobs.values.sortedByDescending { it.id }

    fun shutdown() = pool.shutdownNow()

    private fun run(job: JobRecord) {
        val req = job.request
        job.startedAt = Instant.now()
        job.status = "BUILDING"
        try {
            val lines = req.input.lines().filter { it.isNotBlank() }
            val env = StreamExecutionEnvironment.createLocalEnvironment(req.parallelism, Configuration())
            env.setRuntimeMode(if (req.batchMode) RuntimeExecutionMode.BATCH else RuntimeExecutionMode.STREAMING)
            job.log("Local Flink mini-cluster environment created (parallelism=${req.parallelism}, " +
                "mode=${if (req.batchMode) "BATCH" else "STREAMING"})")

            val stream = req.scenario.build(ScenarioContext(env, lines, req.parallelism, req.watermarkDelaySec))
            val iterator = stream.collectAsync()
            job.plan = mapper.readTree(env.executionPlan)
            job.log("Pipeline built; execution plan generated (see the Plan tab)")

            val client = env.executeAsync("${req.scenario.id}-#${job.id}")
            job.flinkJobId = client.jobID.toString()
            job.log("Submitted to Flink as job ${client.jobID}")

            val collector = Thread {
                iterator.use { while (it.hasNext()) job.addResult(it.next()) }
            }.apply { isDaemon = true; start() }

            val done = client.jobExecutionResult
            var last: JobStatus? = null
            while (!done.isDone) {
                // The mini-cluster shuts down right after the job ends, so status polling may fail near the end.
                val s = runCatching { client.jobStatus.get(1, java.util.concurrent.TimeUnit.SECONDS) }.getOrNull()
                if (s != null && s != last) {
                    job.log("Flink job state: $s")
                    job.status = s.name
                    last = s
                }
                Thread.sleep(20)
            }
            done.get() // throws if the job failed
            collector.join(10_000)
            if (last != JobStatus.FINISHED) job.log("Flink job state: FINISHED")
            job.status = "FINISHED"
            job.log("Collected ${job.resultCount} result record(s)")
        } catch (e: Throwable) {
            job.status = "FAILED"
            job.error = generateSequence<Throwable>(e) { it.cause }.last().let { "${it.javaClass.simpleName}: ${it.message}" }
            job.log("Failed: ${job.error}")
        } finally {
            job.finishedAt = Instant.now()
        }
    }
}
