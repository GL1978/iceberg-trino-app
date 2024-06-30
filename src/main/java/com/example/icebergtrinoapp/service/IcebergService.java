package com.example.icebergtrinoapp.service;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class IcebergService {

    private final Catalog catalog;
    private final DataSource trinoDataSource;

    @Autowired
    public IcebergService(Catalog catalog, DataSource trinoDataSource) {
        this.catalog = catalog;
        this.trinoDataSource = trinoDataSource;
    }

    public void bulkInsert(List<Map<String, Object>> records, String tableName) throws Exception {
        Table table = catalog.loadTable(TableIdentifier.of("default", tableName));

        // Construct the SQL insert statement
        StringBuilder insertSql = new StringBuilder("INSERT INTO iceberg.default." + tableName + " (");
        for (Types.NestedField field : table.schema().columns()) {
            insertSql.append(field.name()).append(", ");
        }
        insertSql.setLength(insertSql.length() - 2); // Remove trailing comma and space
        insertSql.append(") VALUES (");
        for (int i = 0; i < table.schema().columns().size(); i++) {
            insertSql.append("?, ");
        }
        insertSql.setLength(insertSql.length() - 2); // Remove trailing comma and space
        insertSql.append(")");

        try (Connection connection = trinoDataSource.getConnection();
             PreparedStatement pstmt = connection.prepareStatement(insertSql.toString())) {

            for (Map<String, Object> record : records) {
                int index = 1;
                for (Types.NestedField field : table.schema().columns()) {
                    pstmt.setObject(index++, record.get(field.name()));
                }
                pstmt.addBatch();
            }

            pstmt.executeBatch();
        }
    }

    public List<Map<String, Object>> queryIcebergTable(String tableName) throws Exception {
        try (Connection connection = trinoDataSource.getConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM iceberg.default." + tableName)) {

            List<Map<String, Object>> records = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> record = new LinkedHashMap<>();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    record.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                }
                records.add(record);
            }
            return records;
        }
    }
}
