package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //1.缓存穿透解决方案-缓存空对象
        //Shop shop = queryWithPassThrough(id);
        Shop shop = cacheClient.getWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //2.缓存击穿解决方案-互斥锁（同时也用到缓存空对象解决缓存穿透）
        //Shop shop = queryWithPassThrough(id);

        //3.缓存击穿解决方案-逻辑过期时间
        // 【热点数据预热，不命中直接返回错误（这里只处理热点数据），数据修改这里应该就不能在redis删除l，因为删除下次不命中直接返回null】
        // 测试类中有向redis预热数据的方法，搭配一起测试，否则直接不命中
        //Shop shop = queryWithLogicExpire(id);
        //Shop shop=cacheClient.getWithLogicExpire(CACHE_SHOP_KEY,id,Shop.class,this::getById,20L,TimeUnit.SECONDS);

        if(shop==null){
            return Result.fail("店铺不存在!");
        }
        return Result.ok(shop);
    }

    public Shop queryWithPassThrough(Long id){
        String key=CACHE_SHOP_KEY+id;
        //从redis中查询店铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的是否为空值
        if(shopJson!=null){
            //为 "",不能查询数据库了
            return null;
        }
        //不存在，查询数据库
        Shop shop = getById(id);
        //判断是否为空
        if(shop==null){
            //为空，在redis存入“”字符串(存空值解决缓存穿透；还有布隆过滤方法)
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //不为空，将店铺信息存入redis  设置过期时间
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //返回
        return shop;
    }

    public Shop queryWithMutex(Long id){
        String key=CACHE_SHOP_KEY+id;
        //从redis中查询店铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的是否为空值
        if(shopJson!=null){
            //为 "",不能查询数据库了
            return null;
        }
        //实现缓存重建
        //尝试获取互斥锁
        String lock_key=LOCK_SHOP_KEY+id;
        Shop shop=null;
        try {
            boolean isLock = tryLock(lock_key);
            //判断是否获取锁成功
            if(!isLock){
                //失败,休眠，并重试，直到缓存中有数据
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //获取互斥锁成功，查询数据库
            shop = getById(id);
            //判断是否为null
            if(shop==null){
                //为空，在redis存入“”字符串(存空值解决缓存穿透；还有布隆过滤方法)
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            //不为空，将店铺信息存入redis  设置过期时间
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //释放互斥锁
            unLock(lock_key);
        }
        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);

    public Shop queryWithLogicExpire(Long id){
        String key=CACHE_SHOP_KEY+id;
        //从redis中查询店铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //不存在，直接返回
            return null;
        }
        //命中,把json反序列化为bean对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop=JSONUtil.toBean(data,Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //检验是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期，直接返回店铺信息
            return shop;
        }
        //已过期，需要缓存重建
        //获取互斥锁
        String lock_key=LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lock_key);
        //判断是否获取锁成功
        if(isLock){
            //成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unLock(lock_key);
                }

            });
        }

        //返回过期的商铺信息
        return shop;
    }

    public void saveShop2Redis(Long id, Long expireTime){
        //查询数据库
        Shop shop = getById(id);
        //封装过期时间
        RedisData redisData=new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        //写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }

    //获取互斥锁
    public boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //释放互斥锁
    public void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result updated(Shop shop) {
        Long id = shop.getId();
        if(id==null){
            //id不存在，返回错误信息
            return Result.fail("id不能为空");
        }
        //修改店铺信息
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        //返回
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否要根据坐标查询 【这里可能按人气等查询，这里没实现，不按位置查询就按默认方式查询】
        if(x==null || y==null){
            // 不需要坐标查询，按数据库查询
            Page<Shop> page=query().eq("type_id",typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            //返回数据
            return Result.ok(page.getRecords());
        }

        //2.计算分页参数
        int from=(current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end=(current)*SystemConstants.DEFAULT_PAGE_SIZE;

        //3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key=SHOP_GEO_KEY+typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        if(results==null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size()<=from){
            //没有下一页数据了，返回
            return Result.ok(Collections.emptyList());
        }
        //4.解析出id
        List<Long> ids=new ArrayList<>(list.size());
        Map<Long,Distance> map=new HashMap<>(list.size());
        //截取from~end的部分
        list.stream().skip(from).forEach(res->{
            Long shopId = Long.valueOf(res.getContent().getName());
            ids.add(shopId);
            Distance distance = res.getDistance();
            map.put(shopId,distance);
        });

        //5.根据id查询shop
        String join = StrUtil.join(",", ids);
        List<Shop> shopList = query().in("id", ids).last("ORDER BY FIELD(id," + join + ")").list();
        for (Shop shop : shopList) {
            shop.setDistance(map.get(shop.getId()).getValue());
        }

        //6.返回
        return Result.ok(shopList);
    }
}
