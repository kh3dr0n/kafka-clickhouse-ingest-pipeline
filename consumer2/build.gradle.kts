import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar // Import for shadowJar

plugins {
    kotlin("jvm") version "1.9.21" // Use a recent Kotlin version
    kotlin("plugin.serialization") version "1.9.21"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1" // For creating fat JARs
}

group = "com.yourcompany"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Kotlin & Coroutines
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.7.3") // For CompletableFuture integration if needed

    // Arrow KT
    val arrowVersion = "1.2.1" // Check for the latest Arrow version
    implementation("io.arrow-kt:arrow-core:$arrowVersion")
    implementation("io.arrow-kt:arrow-fx-coroutines:$arrowVersion")
    // implementation("io.arrow-kt:arrow-fx-stm:$arrowVersion") // Optional, if using STM

    // Kafka Client
    implementation("org.apache.kafka:kafka-clients:3.6.1") // Check for latest compatible version

    // ClickHouse Client (JDBC)
    // Using JDBC for simplicity, wrapped in Dispatchers.IO
    // Consider clickhouse-java native client for potentially better async perf if needed.
    implementation("com.clickhouse:clickhouse-jdbc:0.5.0") // Check for latest version, 'http' classifier needed for HTTP protocol
    implementation("com.zaxxer:HikariCP:5.1.0") // Connection Pooling

    // JSON Serialization (Kotlinx)
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.2")

    // Logging (SLF4J + Logback)
    implementation("org.slf4j:slf4j-api:2.0.9")
    runtimeOnly("ch.qos.logback:logback-classic:1.4.11")

    // Testing - JUnit 5, AssertJ, MockK, Testcontainers
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.1")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("io.mockk:mockk:1.13.8")
    testImplementation("org.testcontainers:testcontainers:1.19.3")
    testImplementation("org.testcontainers:junit-jupiter:1.19.3")
    testImplementation("org.testcontainers:kafka:1.19.3")
    testImplementation("org.testcontainers:clickhouse:1.19.3")
}

application {
    mainClass.set("com.yourcompany.kafka.clickhouse.AppKt") // Set the main class
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17" // Target JVM 11 or higher
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

// Configure shadowJar task to create an executable fat JAR
tasks.withType<ShadowJar> {
    archiveBaseName.set("kotlin-kafka-clickhouse")
    archiveClassifier.set("") // No classifier like '-all'
    archiveVersion.set(project.version.toString())
    manifest {
        attributes(mapOf("Main-Class" to application.mainClass.get()))
    }
}

// Ensure shadowJar runs when building
tasks.named("build") {
    dependsOn(tasks.named("shadowJar"))
}