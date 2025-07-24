package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
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

public CacheClient(StringRedisTemplate stringRedisTemplate) {
    this.stringRedisTemplate = stringRedisTemplate;
}
public void set(String key, Object value, Long time, TimeUnit unit) {
    stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
}

public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
    //1.设置逻辑过期
    RedisData redisData = new RedisData();
    redisData.setData(value);
    redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
    //2.写入Redis
    stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
}
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isNotBlank(json)){
            //3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        //判断命中的是否是空值
        if(json != null){
            //返回错误
            return null;
        }
        //4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        //5.不存在，返回错误
        if(r == null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误
            return null;
        }
        //6.存在，写入redis
        this.set(key, r, time, unit);
        //7.返回
        return r;
    }

    public <R,ID> R queryWithLogicalExpire(
            String keyPrefix, ID id,Class<R> type,Function<ID,R> dbFallback,Long time, TimeUnit unit) {
        // 在开头输出接收到的参数
        log.debug("=== queryWithLogicalExpire 方法调用 ===");
        log.debug("参数 keyPrefix: {}", keyPrefix);
        log.debug("参数 id: {}", id);
        log.debug("参数 type: {}", type.getSimpleName());
        log.debug("参数 time: {}", time);
        log.debug("参数 unit: {}", unit);
        log.debug("=== 参数输出结束 ===");

        String key = keyPrefix + id;
        log.debug("构建完整key: {}", key);

        //1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        log.debug("从Redis获取到的json: {}", json);

        //2.判断是否存在
        if(StrUtil.isBlank(json)){
            log.debug("缓存未命中，key: {}", key);
            //3.不存在，返回null
            return null;
        }

        //4.命中。需要先把json反序列化为对象
        log.debug("缓存命中，开始反序列化，key: {}", key);
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        log.debug("RedisData反序列化完成，key: {}, redisData: {}", key, redisData);

        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        log.debug("数据反序列化完成，key: {}, data: {}", key, r);

        LocalDateTime expireTime = redisData.getExpireTime();
        log.debug("获取过期时间，key: {}, expireTime: {}", key, expireTime);

        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期，直接返回店铺信息
            log.debug("缓存未过期，直接返回数据，key: {}", key);
            return r;
        }

        log.debug("缓存已过期，需要重建，key: {}", key);
        //5.2已过期，需要缓存重建

        //6.缓存重建
        //6.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        log.debug("尝试获取锁，lockKey: {}", lockKey);

        boolean isLock = tryLock(lockKey);
        log.debug("获取锁结果，lockKey: {}, result: {}", lockKey, isLock);

        //6.2判断是否获取成功
        if(isLock){
            log.debug("获取锁成功，开启独立线程重建缓存，lockKey: {}", lockKey);
            //6.3成功，开启独立线程，实现缓冲重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                log.debug("开始缓存重建，key: {}", key);
                //6.3.1缓存重建
                try {
                    //查询数据库
                    log.debug("开始查询数据库，key: {}", key);
                    R newR = dbFallback.apply(id);
                    log.debug("数据库查询完成，key: {}, result: {}", key, newR);

                    if (newR == null) {
                        log.debug("数据库查询结果为空，key: {}", key);
                    }

                    //写入redis
                    log.debug("开始写入Redis，key: {}", key);
                    this.setWithLogicalExpire(key, newR, time, unit);
                    log.debug("Redis写入完成，key: {}", key);
                } catch (Exception e) {
                    log.error("缓存重建异常，key: {}", key, e);
                    throw new RuntimeException(e);
                } finally {
                    //6.3.2释放锁
                    log.debug("释放锁，lockKey: {}", lockKey);
                    unLock(lockKey);
                }
            });
        } else {
            log.debug("获取锁失败，可能其他线程正在重建缓存，key: {}", key);
        }

        //7.返回过期数据
        log.debug("返回过期数据，key: {}", key);
        return r;
    }

/*    public <R,ID> R queryWithLogicalExpire(String keyPrefix, ID id,Class<R> type,Function<ID,R> dbFallback,Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isBlank(json)){
            //3.存在，直接返回
            return null;
        }
        //4.命中。需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if(expireTime != null && expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期，直接返回店铺信息
            return r;
        }
*//*        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1未过期，直接返回店铺信息
            return r;
        }*//*
        //5.2已过期，需要缓存重建

        //6.缓存重建
        //6.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //6.2判断是否获取成功
        if(isLock){
            //6.3成功，开启独立线程，实现缓冲重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //6.3.1缓存重建
                try {
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //6.3.2释放锁
                    unLock(lockKey);
                }
            });
        }
        //7.返回
        return r;
    }*/
public <R, ID> R queryWithMutex(
        String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
    String key = keyPrefix + id;
    // 1.从redis查询商铺缓存
    String shopJson = stringRedisTemplate.opsForValue().get(key);
    // 2.判断是否存在
    if (StrUtil.isNotBlank(shopJson)) {
        // 3.存在，直接返回
        return JSONUtil.toBean(shopJson, type);
    }
    // 判断命中的是否是空值
    if (shopJson != null) {
        // 返回一个错误信息
        return null;
    }

    // 4.实现缓存重建
    // 4.1.获取互斥锁
    String lockKey = LOCK_SHOP_KEY + id;
    R r = null;
    try {
        boolean isLock = tryLock(lockKey);
        // 4.2.判断是否获取成功
        if (!isLock) {
            // 4.3.获取锁失败，休眠并重试
            Thread.sleep(50);
            return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
        }
        // 4.4.获取锁成功，根据id查询数据库
        r = dbFallback.apply(id);
        // 5.不存在，返回错误
        if (r == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }
        // 6.存在，写入redis
        this.set(key, r, time, unit);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    }finally {
        // 7.释放锁
        unLock(lockKey);
    }
    // 8.返回
    return r;
}

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}
