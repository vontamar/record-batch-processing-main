package com.multirecordprocess.controller;

import com.multirecordprocess.service.RecordService;
import com.multirecordprocess.service.RecordService2;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/records")
public class RecordController {

    private final RecordService recordService;

    private final RecordService2 recordService2;


    public RecordController(RecordService recordService, RecordService2 recordService2) {
        this.recordService = recordService;
        this.recordService2 = recordService2;
    }

    //this endpoint for testing
    @GetMapping("/ids")
    public ResponseEntity<List<Long>> getIds() {
        return ResponseEntity.ok(recordService.getProductIds());
    }

    //this endpoint for data reset
    @PostMapping("/reset")
    public ResponseEntity<String> resetProductRecords() {
        String response = recordService.resetRecords();
        return ResponseEntity.ok(response);
    }

    @PostMapping("/process")
    public ResponseEntity<String> processProductIds(@RequestBody List<Long> productIds) {
        recordService.processProductIds(productIds);
        return ResponseEntity.ok("Products processed and events published.");
    }

    @PostMapping("/process/v2")
    public ResponseEntity<String> processProductIdsV2(@RequestBody List<Long> productIds) {
        recordService2.executeProductIds(productIds);
        return ResponseEntity.ok("Products processed and events published.");
    }



}
