package com.tamvan.demo.controller;

import com.tamvan.demo.processor.Processor;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/redis")
@RequiredArgsConstructor
public class Controller {
    private final Processor processor;

    @PostMapping("/pipeline")
    public HttpEntity<String> testPipeline(@RequestParam Integer total) {
        long startTime = System.currentTimeMillis();
        processor.testPipeline(total);
        long endTime = System.currentTimeMillis();
        long seconds = (endTime - startTime);
        return new ResponseEntity<>("Duration : " + seconds + " milliseconds", HttpStatus.OK);
    }

    @PostMapping("/manual")
    public HttpEntity<String> testManual(@RequestParam Integer total) {
        long startTime = System.currentTimeMillis();
        processor.testManual(total);
        long endTime = System.currentTimeMillis();
        long seconds = (endTime - startTime) ;
        return new ResponseEntity<>("Duration : " + seconds + " milliseconds", HttpStatus.OK);
    }

    @PostMapping("/multi")
    public HttpEntity<String> testMulti(@RequestParam Integer total) {
        long startTime = System.currentTimeMillis();
        processor.testUsingMulti(total);
        long endTime = System.currentTimeMillis();
        long seconds = (endTime - startTime);
        return new ResponseEntity<>("Duration : " + seconds + " milliseconds", HttpStatus.OK);
    }
}
