package com.tamvan.demo.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class Processor {

    private final RedisTemplate redisTemplate;

    public void testPipeline(int total){
        redisTemplate.executePipelined((RedisCallback) connection -> {
            for(int i=0; i<total; i++){
                connection.commands().set(("Test-"+i).getBytes(), ("Tamvan"+i).getBytes(), Expiration.seconds(60), RedisStringCommands.SetOption.upsert());
            }
            return null;
        });
    }

    public void testManual(int total){
        for(int i=0; i<total; i++){
            redisTemplate.setKeySerializer(new StringRedisSerializer());
            redisTemplate.setValueSerializer(new StringRedisSerializer());
            ValueOperations valueOperations = redisTemplate.opsForValue();
            valueOperations.set(("Test-"+i), ("Tamvan"+i), 60, TimeUnit.SECONDS);
        }
    }

}
