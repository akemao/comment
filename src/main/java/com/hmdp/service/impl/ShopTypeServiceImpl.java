package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryList() {
        //查询redis是否存在商铺
        List<String> shopTypeListInRedis = stringRedisTemplate.opsForList().range(CACHE_SHOP_TYPE, 0, -1);
        //判断是否存在
        if(!shopTypeListInRedis.isEmpty()){
            //存在，直接返回数据
            List<ShopType> shopTypeList=shopTypeListInRedis.stream().map(shopStr->{
                return JSONUtil.toBean(shopStr,ShopType.class);
            }).collect(Collectors.toList());
            return Result.ok(shopTypeList);
        }
        //不存在，查询数据库
        //List<ShopType> shopTypeList = lambdaQuery().orderByAsc(ShopType::getSort).list(); 可能是版本原因报错
        List<ShopType> shopTypeList =query().orderByAsc("sort").list();
        //判断是否为空
        if(shopTypeList.isEmpty()){
            //为空，返回错误信息
            return Result.fail("店铺类型为空");
        }
        //不为空，存入redis
        shopTypeListInRedis=shopTypeList.stream().map(item->{
            return JSONUtil.toJsonStr(item);
        }).collect(Collectors.toList());
        stringRedisTemplate.opsForList().rightPushAll(CACHE_SHOP_TYPE,shopTypeListInRedis);
        //返回
        return Result.ok(shopTypeList);
    }
}
