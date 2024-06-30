package com.example.icebergtrinoapp.config;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class IcebergCatalogConfig {

    @Value("${iceberg.catalog.uri}")
    private String catalogUri;

    @Value("${iceberg.catalog.driver}")
    private String catalogDriver;

    @Value("${iceberg.catalog.user}")
    private String catalogUser;

    @Value("${iceberg.catalog.password}")
    private String catalogPassword;

    @Value("${iceberg.catalog.warehouse}")
    private String catalogWarehouse;

    @Bean
    public Catalog icebergCatalog() {
        Map<String,String> properties = new HashMap<>();
        properties.put("uri", catalogUri);
        properties.put("driver", catalogDriver);
        properties.put("user", catalogUser);
        properties.put("password", catalogPassword);
        properties.put("warehouse", catalogWarehouse);

        Catalog catalog = new JdbcCatalog();
        catalog.initialize("my_jdbc_catalog", properties);
        return catalog;
    }
}

