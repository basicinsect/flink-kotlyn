package dev.flinklab

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ScenarioTest {
    private val manager = JobManager(2)

    private fun runJob(id: String, mutate: (MutableMap<String, Any?>) -> Unit = {}): Map<String, Any?> {
        val body = mutableMapOf<String, Any?>("scenario" to id)
        mutate(body)
        val job = manager.submit(parseRequest(body))
        repeat(600) {
            if (job.finishedAt != null) return job.toMap(true)
            Thread.sleep(100)
        }
        error("job $id did not finish")
    }

    @Suppress("UNCHECKED_CAST")
    private fun results(m: Map<String, Any?>) = m["results"] as List<String>

    @Test
    fun everyScenarioRunsWithDefaults() {
        for (s in Scenarios.all) {
            val r = runJob(s.id)
            assertEquals("FINISHED", r["status"], "${s.id}: ${r["error"]}")
            assertTrue(results(r).isNotEmpty(), "${s.id} produced no output")
            assertNotNull(r["plan"])
        }
    }

    @Test
    fun wordCountBatchGivesFinalCounts() {
        val r = runJob("wordcount") { it["mode"] = "BATCH"; it["input"] = "a b a\nb a" }
        assertEquals(setOf("a -> 3", "b -> 2"), results(r).toSet())
    }

    @Test
    fun lateEventsGoToSideOutput() {
        val r = runJob("late-data")
        assertTrue(results(r).any { it.startsWith("LATE event key=a time=3s") })
    }

    @Test
    fun lateEventsAreOnTimeWithLargeDelay() {
        val r = runJob("late-data") { it["watermarkDelaySec"] = 30 }
        assertTrue(results(r).none { it.startsWith("LATE") })
    }

    @Test
    fun tumblingWindowAggregates() {
        val r = runJob("tumbling") { it["input"] = "k,1,10\nk,2,5\nk,11,7" }
        assertEquals(
            setOf(
                "WINDOW [0s, 10s) key=k count=2 sum=15 max=10 events=[1, 2]",
                "WINDOW [10s, 20s) key=k count=1 sum=7 max=7 events=[11]",
            ),
            results(r).toSet(),
        )
    }

    @Test
    fun invalidRequestsAreRejected() {
        assertFailsWith<IllegalArgumentException> { parseRequest(mapOf("scenario" to "nope")) }
        assertFailsWith<IllegalArgumentException> { parseRequest(mapOf("scenario" to "wordcount", "parallelism" to 99)) }
        assertFailsWith<IllegalArgumentException> { parseRequest(mapOf("scenario" to "wordcount", "input" to " ")) }
    }
}
