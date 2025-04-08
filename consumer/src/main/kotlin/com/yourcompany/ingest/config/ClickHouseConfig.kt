package com.yourcompany.ingest.config

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import javax.sql.DataSource

@Configuration
class ClickHouseConfig {

    // Define properties specifically for the ClickHouse datasource
    @Bean
    @Primary // Mark this as the primary DataSourceProperties if you only have one DB
    @ConfigurationProperties("spring.datasource.clickhouse")
    fun clickhouseDataSourceProperties(): DataSourceProperties {
        return DataSourceProperties()
    }

    // Create the DataSource bean using the properties
    @Bean
    @Primary // Mark this as the primary DataSource if you only have one DB
    @ConfigurationProperties("spring.datasource.clickhouse.hikari") // Apply Hikari props
    fun clickhouseDataSource(clickhouseDataSourceProperties: DataSourceProperties): DataSource {
        return clickhouseDataSourceProperties
            .initializeDataSourceBuilder()
            // .type(HikariDataSource::class.java) // Usually auto-detected
            .build()
    }

    // Create a JdbcTemplate bean specifically for ClickHouse
    @Bean
    fun clickhouseJdbcTemplate(clickhouseDataSource: DataSource): JdbcTemplate {
        return JdbcTemplate(clickhouseDataSource)
    }
}