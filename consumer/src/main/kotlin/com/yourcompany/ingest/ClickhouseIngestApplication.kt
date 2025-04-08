package com.yourcompany.ingest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ClickhouseIngestApplication

fun main(args: Array<String>) {
    runApplication<ClickhouseIngestApplication>(*args)
}