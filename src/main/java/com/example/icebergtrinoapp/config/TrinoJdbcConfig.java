package com.example.icebergtrinoapp.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@Configuration
public class TrinoJdbcConfig {

    @Value("${trino.jdbc.url}")
    private String trinoUrl;

    @Value("${trino.jdbc.user}")
    private String trinoUser;

    @Value("${trino.jdbc.password}")
    private String trinoPassword;

    @Bean
    public DataSource trinoDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(trinoUrl);
        config.setUsername(trinoUser);
        config.setPassword(trinoPassword);
        return new HikariDataSource(config);
    }
}
