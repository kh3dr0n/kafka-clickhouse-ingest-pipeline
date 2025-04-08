import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.springframework.boot") version "3.2.4" // Use a recent stable Spring Boot version
    id("io.spring.dependency-management") version "1.1.4"
    kotlin("jvm") version "1.9.23" // Use a recent stable Kotlin version
    kotlin("plugin.spring") version "1.9.23"
}

group = "com.yourcompany"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17 // Or newer (21 recommended)
}

repositories {
    mavenCentral()
}

dependencies {
    // Spring Boot Core
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-actuator") // For health checks etc.

    // Kotlin
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib") // Explicit stdlib dependency

    // Kafka
    implementation("org.springframework.kafka:spring-kafka")

    // JSON Processing (Jackson - included transitively by spring-boot-starter)
    // implementation("com.fasterxml.jackson.module:jackson-module-kotlin") // Included by default now

    // ClickHouse & JDBC
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    // Use the official recommended ClickHouse JDBC driver
    // implementation("ru.yandex.clickhouse:clickhouse-jdbc:0.5.1") // Older driver
    implementation("com.clickhouse:clickhouse-jdbc:0.6.0") // Newer driver (use 'http' classifier for basic http interface) {
    {
       // classifier = "http" // If using HTTP interface, else omit for native TCP
    }


    // Configuration Processor (optional, helps IDE with application.yml)
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    // Testing (optional but recommended)
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
}

dependencyManagement {
    imports {
        mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES)
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs += "-Xjsr305=strict"
        jvmTarget = "17" // Match java.sourceCompatibility
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// Make sure Spring Boot builds an executable JAR
tasks.getByName<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    enabled = true
}

tasks.getByName<org.springframework.boot.gradle.tasks.run.BootRun>("bootRun") {
    enabled = true
}