package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    private static final long BEGIN_TIMESTAMP=1704070860L;
    private StringRedisTemplate stringRedisTemplate;
    private static final long MOVE_COUNT=32;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate=stringRedisTemplate;
    }

    public long nextId(String keyPrefix){
        //生成时间戳
        LocalDateTime time=LocalDateTime.now();
        long nowSecond = time.toEpochSecond(ZoneOffset.UTC);
        long timestamp=nowSecond-BEGIN_TIMESTAMP;
        //生成序列号
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy:MM:dd");
        String date = LocalDateTime.now().format(formatter);
        Long serialNumber = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix+ ":" + date);
        //拼接并返回
        long id=(timestamp<<MOVE_COUNT)|serialNumber;
        return id;
    }



 /*   public static void main(String[] args) {
        LocalDateTime time = LocalDateTime.of(2024, 1, 1, 1, 1);
        long second = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println(second);
    }*/
}
