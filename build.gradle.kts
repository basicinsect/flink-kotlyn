plugins {
    kotlin("jvm") version "2.1.20"
    application
}

repositories {
    mavenCentral()
}

val flinkVersion = "1.20.1"

// Flink serializes user functions, so Kotlin lambdas/SAM conversions must compile to real classes
// (the default invokedynamic lambdas are not reliably serializable by Flink's closure cleaner).
kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xlambdas=class", "-Xsam-conversions=class")
    }
}

dependencies {
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    implementation("org.apache.flink:flink-runtime:$flinkVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    runtimeOnly("org.slf4j:slf4j-simple:1.7.36")

    testImplementation(kotlin("test"))
}

// Flink's reflection-heavy serialization needs these on JDK 17+.
val flinkJvmArgs = listOf(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/java.text=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
)

application {
    mainClass.set("dev.flinklab.MainKt")
    applicationDefaultJvmArgs = flinkJvmArgs
}

tasks.test {
    useJUnitPlatform()
    jvmArgs(flinkJvmArgs)
}
