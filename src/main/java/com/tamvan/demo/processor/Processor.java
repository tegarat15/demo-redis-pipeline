package com.tamvan.demo.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class Processor {

    private final RedisTemplate redisTemplate;

    public void testPipeline(int total){
        List<Map<String, String>> list = getMaps(total, "Tamvan-pipeline-");
        AtomicInteger loop = new AtomicInteger(0);
        list.forEach(x->{
            log.info("Loop Pipeline:"+loop.incrementAndGet());
            redisTemplate.executePipelined((RedisCallback) connection -> {
                x.forEach((k,v)->{
                    connection.commands().set(k.getBytes(), v.getBytes(), Expiration.seconds(120), RedisStringCommands.SetOption.UPSERT);
                });
                return null;
            });
        });
    }

    public void testManual(int total){
        AtomicInteger loop = new AtomicInteger(0);
        for(int i=0; i<total; i++){
            log.info("Loop:"+loop.incrementAndGet());
            redisTemplate.setKeySerializer(new StringRedisSerializer());
            redisTemplate.setValueSerializer(new StringRedisSerializer());
            ValueOperations valueOperations = redisTemplate.opsForValue();
            valueOperations.set(("Test-"+i), ("Tamvan-"+i), 120, TimeUnit.SECONDS);
        }
    }

    public void testUsingMulti(int total){
        List<Map<String, String>> list = getMaps(total, "Tamvan-multi-");
        AtomicInteger loop = new AtomicInteger(0);
        list.forEach(x->{
            log.info("Loop Multi:"+loop.incrementAndGet());
            redisTemplate.setKeySerializer(new StringRedisSerializer());
            redisTemplate.setValueSerializer(new StringRedisSerializer());
            redisTemplate.execute(new SessionCallback<List<Object>>() {
                @Override
                public <K, V> List<Object> execute(RedisOperations<K, V> operations) throws DataAccessException {
                    operations.multi();
                    for (Map.Entry<String, String> entry : x.entrySet()) {
                        operations.opsForValue().set((K) entry.getKey(), (V) entry.getValue(), 120, TimeUnit.SECONDS);
                    }
                    return operations.exec();
                }
            });
        });
    }

    private static List<Map<String, String>> getMaps(int total, String x) {
        List<Map<String, String>> list = new ArrayList<>();
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < total; i++) {
            map.put("Test-" + i, x + i);
            if (i % 10000 == 0 && i != 0) {
                list.add(map);
                map = new HashMap<>();
            }
        }
        if (!map.isEmpty()) {
            list.add(map);
        }
        return list;
    }
}
