package dev.flinklab

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.eventtime.Watermark
import org.apache.flink.api.common.eventtime.WatermarkGenerator
import org.apache.flink.api.common.eventtime.WatermarkOutput
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag

/** Everything a scenario needs to build its pipeline. */
class ScenarioContext(
    val env: StreamExecutionEnvironment,
    val lines: List<String>,
    val parallelism: Int,
    val watermarkDelaySec: Long,
)

class Scenario(
    val id: String,
    val title: String,
    val summary: String,
    val concepts: List<String>,
    val inputHint: String,
    val defaultInput: String,
    val eventTime: Boolean,
    val defaultParallelism: Int,
    val build: (ScenarioContext) -> DataStream<String>,
)

typealias Reading = Tuple3<String, Long, Int> // (key, event-time seconds, value)

object Scenarios {
    val all: List<Scenario> = listOf(
        Scenario(
            id = "wordcount",
            title = "Word count (streaming vs batch)",
            summary = "The classic. Lines are split into words, keyed by word and summed. In STREAMING mode " +
                "Flink emits an updated count for every record; in BATCH mode (same code!) only final counts appear.",
            concepts = listOf("flatMap", "keyBy", "rolling sum", "unified batch/streaming runtime"),
            inputHint = "Free text, one line per record.",
            defaultInput = "to be or not to be\nthat is the question\nto flink or not to flink",
            eventTime = false,
            defaultParallelism = 2,
            build = ::wordCount,
        ),
        Scenario(
            id = "partitioning",
            title = "Parallelism & partitioning",
            summary = "Shows which parallel subtask handles each record. rebalance() spreads records round-robin, " +
                "keyBy() sends equal keys to the same subtask. Raise the parallelism to see the effect.",
            concepts = listOf("parallelism", "rebalance", "keyBy hash partitioning", "subtasks"),
            inputHint = "Words separated by spaces or newlines.",
            defaultInput = "apple banana cherry apple banana apple date elderberry cherry fig grape apple",
            eventTime = false,
            defaultParallelism = 4,
            build = ::partitioning,
        ),
        Scenario(
            id = "tumbling",
            title = "Tumbling event-time windows",
            summary = "Fixed, non-overlapping 10 second windows over event time, per key. " +
                "Each result shows the window bounds, count, sum and max.",
            concepts = listOf("event time", "watermarks", "tumbling windows", "ProcessWindowFunction"),
            inputHint = "One event per line: key,eventTimeSeconds,value",
            defaultInput = READINGS,
            eventTime = true,
            defaultParallelism = 2,
            build = { windowed(it, TumblingEventTimeWindows.of(Time.seconds(10))) },
        ),
        Scenario(
            id = "sliding",
            title = "Sliding event-time windows",
            summary = "20 second windows that slide every 10 seconds, so each event lands in two windows.",
            concepts = listOf("sliding windows", "overlapping windows", "event time"),
            inputHint = "One event per line: key,eventTimeSeconds,value",
            defaultInput = READINGS,
            eventTime = true,
            defaultParallelism = 2,
            build = { windowed(it, SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(10))) },
        ),
        Scenario(
            id = "session",
            title = "Session windows",
            summary = "Windows that close after a 7 second gap of inactivity per key; window sizes are data driven.",
            concepts = listOf("session windows", "inactivity gap", "dynamic windows"),
            inputHint = "One event per line: key,eventTimeSeconds,value",
            defaultInput = READINGS,
            eventTime = true,
            defaultParallelism = 2,
            build = { windowed(it, EventTimeSessionWindows.withGap(Time.seconds(7))) },
        ),
        Scenario(
            id = "late-data",
            title = "Late data & side outputs",
            summary = "Events that arrive after the watermark has passed their window are not dropped silently: " +
                "they are routed to a side output (LATE). Increase the watermark delay to make them on-time again.",
            concepts = listOf("watermarks", "late events", "side output", "out-of-orderness"),
            inputHint = "One event per line: key,eventTimeSeconds,value (arrival order matters!)",
            defaultInput = "a,1,1\na,4,1\na,12,1\na,3,1\na,15,1\na,25,1\na,8,1\na,27,1",
            eventTime = true,
            defaultParallelism = 1,
            build = ::lateData,
        ),
        Scenario(
            id = "keyed-state",
            title = "Keyed state: running average",
            summary = "Per-key fault-tolerant state (ValueState) keeps a count and sum, emitting a running average. " +
                "This state is what Flink snapshots in checkpoints.",
            concepts = listOf("keyed state", "ValueState", "KeyedProcessFunction", "checkpointing"),
            inputHint = "One record per line: key,value",
            defaultInput = "alice,10\nbob,4\nalice,20\nbob,8\nalice,30\ncarol,7\nbob,12",
            eventTime = false,
            defaultParallelism = 2,
            build = ::keyedState,
        ),
        Scenario(
            id = "side-outputs",
            title = "Side outputs: split a stream",
            summary = "One ProcessFunction routes records to multiple outputs: valid numbers, negatives and " +
                "unparsable input, without running the stream twice.",
            concepts = listOf("ProcessFunction", "OutputTag", "side outputs", "union"),
            inputHint = "Values separated by commas, spaces or newlines. Try adding text or negatives.",
            defaultInput = "5, 12, -3, hello, 42, -17, 8, oops, 99",
            eventTime = false,
            defaultParallelism = 2,
            build = ::sideOutputs,
        ),
    )

    fun find(id: String): Scenario? = all.firstOrNull { it.id == id }
}

private const val READINGS = "s1,1,10\ns1,4,20\ns2,3,5\ns1,12,30\ns2,14,15\ns2,16,25\ns1,18,40\ns1,35,50\ns2,36,60\ns1,50,70"

private fun source(ctx: ScenarioContext, lines: List<String>): SingleOutputStreamOperator<String> {
    require(lines.isNotEmpty()) { "Input must not be empty" }
    return ctx.env.fromData(lines, Types.STRING).setParallelism(1)
}

private fun tokens(lines: List<String>): List<String> =
    lines.flatMap { it.split(Regex("[\\s,]+")) }.filter { it.isNotBlank() }

// ---------------------------------------------------------------- word count

private class Tokenizer : FlatMapFunction<String, Tuple2<String, Int>> {
    override fun flatMap(value: String, out: Collector<Tuple2<String, Int>>) {
        value.lowercase().split(Regex("\\W+")).filter { it.isNotEmpty() }.forEach { out.collect(Tuple2.of(it, 1)) }
    }
}

private class Field0 : KeySelector<Tuple2<String, Int>, String> {
    override fun getKey(value: Tuple2<String, Int>): String = value.f0
}

private class WordCountFormatter : MapFunction<Tuple2<String, Int>, String> {
    override fun map(value: Tuple2<String, Int>): String = "${value.f0} -> ${value.f1}"
}

private fun wordCount(ctx: ScenarioContext): DataStream<String> =
    source(ctx, ctx.lines)
        .flatMap(Tokenizer()).returns(Types.TUPLE(Types.STRING, Types.INT)).name("tokenize")
        .keyBy(Field0(), Types.STRING)
        .sum(1).name("count per word")
        .map(WordCountFormatter()).returns(Types.STRING).name("format")

// -------------------------------------------------------------- partitioning

private class SubtaskTagger : RichMapFunction<String, Tuple2<String, Int>>() {
    override fun map(value: String): Tuple2<String, Int> =
        Tuple2.of(value, runtimeContext.taskInfo.indexOfThisSubtask)
}

private class WordKey : KeySelector<Tuple2<String, Int>, String> {
    override fun getKey(value: Tuple2<String, Int>): String = value.f0
}

private class SubtaskReporter : RichMapFunction<Tuple2<String, Int>, String>() {
    override fun map(value: Tuple2<String, Int>): String =
        "${value.f0}: rebalance -> subtask ${value.f1}, keyBy -> subtask ${runtimeContext.taskInfo.indexOfThisSubtask}"
}

private fun partitioning(ctx: ScenarioContext): DataStream<String> =
    source(ctx, tokens(ctx.lines))
        .rebalance()
        .map(SubtaskTagger()).returns(Types.TUPLE(Types.STRING, Types.INT)).name("tag rebalance subtask")
        .keyBy(WordKey(), Types.STRING)
        .map(SubtaskReporter()).returns(Types.STRING).name("report keyBy subtask")

// ----------------------------------------------------------- event-time bits

private fun parseReading(line: String): Reading? {
    val p = line.split(",").map { it.trim() }
    if (p.size != 3) return null
    val t = p[1].toLongOrNull() ?: return null
    val v = p[2].toIntOrNull() ?: return null
    return Tuple3.of(p[0], t, v)
}

private class ReadingParser : FlatMapFunction<String, Reading> {
    override fun flatMap(value: String, out: Collector<Reading>) {
        if (value.isNotBlank()) parseReading(value)?.let { out.collect(it) }
    }
}

/** Emits a watermark after every event so results do not depend on wall-clock timing. */
private class PerEventWatermarks(private val delayMs: Long) : WatermarkGenerator<Reading> {
    private var maxTs = Long.MIN_VALUE + delayMs + 1

    override fun onEvent(event: Reading, eventTimestamp: Long, output: WatermarkOutput) {
        maxTs = maxOf(maxTs, eventTimestamp)
        output.emitWatermark(Watermark(maxTs - delayMs - 1))
    }

    override fun onPeriodicEmit(output: WatermarkOutput) {}
}

private class SecondsTimestamps : SerializableTimestampAssigner<Reading> {
    override fun extractTimestamp(element: Reading, recordTimestamp: Long): Long = element.f1 * 1000
}

private class WatermarkSupplier(private val delayMs: Long) :
    org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier<Reading> {
    override fun createWatermarkGenerator(
        context: org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context,
    ): WatermarkGenerator<Reading> = PerEventWatermarks(delayMs)
}

private fun readings(ctx: ScenarioContext): SingleOutputStreamOperator<Reading> {
    val strategy = WatermarkStrategy.forGenerator(WatermarkSupplier(ctx.watermarkDelaySec * 1000))
        .withTimestampAssigner(SecondsTimestamps())
    return source(ctx, ctx.lines)
        .flatMap(ReadingParser()).returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT)).name("parse events")
        .setParallelism(1)
        .assignTimestampsAndWatermarks(strategy).name("timestamps & watermarks")
}

private class ReadingKey : KeySelector<Reading, String> {
    override fun getKey(value: Reading): String = value.f0
}

private fun fmt(ms: Long) = "${ms / 1000}s"

private class WindowSummary : ProcessWindowFunction<Reading, String, String, TimeWindow>() {
    override fun process(key: String, context: Context, elements: Iterable<Reading>, out: Collector<String>) {
        val values = elements.map { it.f2 }
        val w = context.window()
        out.collect(
            "WINDOW [${fmt(w.start)}, ${fmt(w.end)}) key=$key count=${values.size} sum=${values.sum()} " +
                "max=${values.max()} events=${elements.map { it.f1 }.sorted()}",
        )
    }
}

private fun <W : org.apache.flink.streaming.api.windowing.windows.Window> windowed(
    ctx: ScenarioContext,
    assigner: org.apache.flink.streaming.api.windowing.assigners.WindowAssigner<in Reading, W>,
): DataStream<String> =
    readings(ctx)
        .keyBy(ReadingKey(), Types.STRING)
        .window(assigner)
        .process(WindowSummary() as ProcessWindowFunction<Reading, String, String, W>, Types.STRING)
        .name("window summary")

private val LATE: OutputTag<Reading> = OutputTag("late", TypeInformation.of(object : org.apache.flink.api.common.typeinfo.TypeHint<Reading>() {}))

private class LateFormatter : MapFunction<Reading, String> {
    override fun map(value: Reading): String = "LATE event key=${value.f0} time=${value.f1}s value=${value.f2} (side output)"
}

private fun lateData(ctx: ScenarioContext): DataStream<String> {
    val main = readings(ctx)
        .keyBy(ReadingKey(), Types.STRING)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .sideOutputLateData(LATE)
        .process(WindowSummary(), Types.STRING).name("window summary")
    val late = main.getSideOutput(LATE).map(LateFormatter()).returns(Types.STRING).name("format late")
    return main.union(late)
}

// --------------------------------------------------------------- keyed state

private class KeyValueParser : FlatMapFunction<String, Tuple2<String, Int>> {
    override fun flatMap(value: String, out: Collector<Tuple2<String, Int>>) {
        val p = value.split(",").map { it.trim() }
        val v = p.getOrNull(1)?.toIntOrNull() ?: return
        if (p[0].isNotEmpty()) out.collect(Tuple2.of(p[0], v))
    }
}

private class RunningAverage : KeyedProcessFunction<String, Tuple2<String, Int>, String>() {
    private lateinit var count: ValueState<Long>
    private lateinit var sum: ValueState<Long>

    override fun open(openContext: OpenContext) {
        count = runtimeContext.getState(ValueStateDescriptor("count", Types.LONG))
        sum = runtimeContext.getState(ValueStateDescriptor("sum", Types.LONG))
    }

    override fun processElement(value: Tuple2<String, Int>, ctx: Context, out: Collector<String>) {
        val c = (count.value() ?: 0L) + 1
        val s = (sum.value() ?: 0L) + value.f1
        count.update(c)
        sum.update(s)
        out.collect("${ctx.currentKey}: value=${value.f1} -> count=$c sum=$s avg=${"%.2f".format(s.toDouble() / c)}")
    }
}

private fun keyedState(ctx: ScenarioContext): DataStream<String> =
    source(ctx, ctx.lines)
        .flatMap(KeyValueParser()).returns(Types.TUPLE(Types.STRING, Types.INT)).name("parse key,value")
        .keyBy(Field0(), Types.STRING)
        .process(RunningAverage(), Types.STRING).name("running average (ValueState)")

// ------------------------------------------------------------- side outputs

private val NEGATIVE: OutputTag<String> = OutputTag("negative", Types.STRING)
private val INVALID: OutputTag<String> = OutputTag("invalid", Types.STRING)

private class Router : ProcessFunction<String, String>() {
    override fun processElement(value: String, ctx: Context, out: Collector<String>) {
        val n = value.toLongOrNull()
        when {
            n == null -> ctx.output(INVALID, "INVALID (side output): '$value' is not a number")
            n < 0 -> ctx.output(NEGATIVE, "NEGATIVE (side output): $n")
            else -> out.collect("VALID (main output): $n")
        }
    }
}

private fun sideOutputs(ctx: ScenarioContext): DataStream<String> {
    val main = source(ctx, tokens(ctx.lines)).process(Router(), Types.STRING).name("route")
    return main.union(main.getSideOutput(NEGATIVE), main.getSideOutput(INVALID))
}
