package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;


@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate=stringRedisTemplate;
    }

    //将任意Java对象存储在redis中（String），设置TTL
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }
    //将任意Java对象存储在redis中（String）,设置逻辑过期时间
    public void setWithLogicExpire(String key,Object value,Long expireTime,TimeUnit unit){
        RedisData redisData=new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(expireTime)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    //查询数据，利用缓存空值解决缓存穿透问题
    public <R,ID> R getWithPassThrough(String prefix, ID id, Class<R> type, Function<ID,R> dbFallback,
                                         Long time,TimeUnit unit){
        String key=prefix+id;
        //从redis查询缓存
        String json= stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isNotBlank(json)){
            //存在,返回
            return JSONUtil.toBean(json, type);

        }
        //判断命中的是否为空值
        if(json!=null){
            //不为null，则为"",此时不能查询数据库了
            return null;
        }
        //查询数据库
        R r = dbFallback.apply(id);
        //判断是否为空
        if(r==null){
            //为空，redis缓存空值，返回null
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //不为空，存入redis
        set(key,r,time,unit);
        return r;
    }

    //查询数据，利用逻辑过期时间解决缓存击穿问题(热点key)
    public <R,ID> R getWithLogicExpire(String prefix,ID id,Class<R> type,Function<ID,R> dbFallback,
                                    Long time,TimeUnit unit){
        String key=prefix+id;
        //根据key查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //查询是否命中
        if(json.isBlank()){
            //未命中，返回错误信息
            return null;
        }
        //命中，反序列化为java对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        //判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期，返回数据
            return r;
        }
        //过期，缓存重建
        //获取锁，判断获取锁是否成功
        String lock_key=LOCK_SHOP_KEY+id;
        boolean isLock=getLock(lock_key);
        if (isLock){
            //成功，开个新线程
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    //查询数据库
                    R res = dbFallback.apply(id);
                    //存入redis
                    setWithLogicExpire(key,res,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    releaseLock(lock_key);
                }
            });

        }
        //返回旧数据
        return r;
    }

    public boolean getLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    public void releaseLock(String key){
        stringRedisTemplate.delete(key);
    }
}
