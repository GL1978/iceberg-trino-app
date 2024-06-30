package com.example.icebergtrinoapp.controller;

import com.example.icebergtrinoapp.service.IcebergService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/iceberg")
public class IcebergController {

    private final IcebergService icebergService;

    @Autowired
    public IcebergController(IcebergService icebergService) {
        this.icebergService = icebergService;
    }

    @Operation(summary = "Bulk insert records into an Iceberg table")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Bulk insert successful"),
            @ApiResponse(responseCode = "500", description = "Bulk insert failed",
                    content = @Content(schema = @Schema(implementation = String.class)))
    })
    @PostMapping("/bulk-insert")
    public ResponseEntity<String> bulkInsert(
            @Parameter(description = "Name of the Iceberg table") @RequestParam String tableName,
            @Parameter(description = "List of records to insert") @RequestBody List<Map<String, Object>> records) {
        try {
            icebergService.bulkInsert(records, tableName);
            return ResponseEntity.ok("Bulk insert successful");
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Bulk insert failed: " + e.getMessage());
        }
    }

    @Operation(summary = "Query records from an Iceberg table")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Query successful",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = Map.class)))),
            @ApiResponse(responseCode = "500", description = "Query failed",
                    content = @Content(schema = @Schema(implementation = String.class)))
    })
    @GetMapping("/query")
    public ResponseEntity<List<Map<String, Object>>> queryIcebergTable(
            @Parameter(description = "Name of the Iceberg table") @RequestParam String tableName) {
        try {
            List<Map<String, Object>> records = icebergService.queryIcebergTable(tableName);
            return ResponseEntity.ok(records);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body(null);
        }
    }
}
