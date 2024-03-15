package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        Page<Blog> page=query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        //获取当前页数据
        List<Blog> records = page.getRecords();
        records.forEach(blog -> {
            //查询用户，并封装用户信息到blog
            queryBlogUser(blog);
            //查询是否点赞，并封装用户信息到blog
            queryIsLiked(blog);
        });
        //返回
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if(blog==null){
            return Result.fail("笔记不存在");
        }
        //查询blog有关的用户,并封装用户信息到blog
        queryBlogUser(blog);
        //查询是否点赞
        queryIsLiked(blog);

        //返回
        return Result.ok(blog);
    }

    private void queryIsLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if(user==null){
            //用户未登录，无需查询是否点赞
            return;
        }
        //获取用户id
        Long userId = user.getId();
        //获取点赞信息
        String key=BLOG_LIKED_KEY+blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score!=null);
    }

    @Override
    public Result likeBlog(Long id) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        //获取点赞信息
        String key=BLOG_LIKED_KEY+id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //查询redis是否有点赞信息
        if(score!=null){
            //有，点赞数-1
            boolean isSuccess = update().setSql("liked=liked-1").eq("id", id).update();
            //删除redis点赞数据
            if(isSuccess){
                //stringRedisTemplate.delete(key); 这是set类型啊，怎么这么糊涂
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }else{
            //没有,数据库点赞数+1
            boolean isSuccess = update().setSql("liked=liked+1").eq("id", id).update();
            //存入redis
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }
        //返回
        return Result.ok();
    }

    /**
     * 查询点赞列表
     * @param id
     * @return
     */
    public Result queryBlogLikes(Long id) {
        String key=BLOG_LIKED_KEY+id;
        //获取点赞前5名id
        Set<String> set = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        //判断集合是否为空
        if(set==null || set.isEmpty()){
            //为空，返回空集合
            return Result.ok(Collections.emptyList());
        }
        //解析其中的用户id
        List<Long> ids = set.stream().map(Long::valueOf).collect(Collectors.toList());
        String idsStr = StrUtil.join(",", ids);
        //根据id查询用户
        List<UserDTO> userDTOList = userService.query()
                .in("id", ids).last("ORDER BY FIELD(id," + idsStr + ")")
                .list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        //返回
        return Result.ok(userDTOList);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryBlogsByIds(Long userId, Integer current) {
        Page<Blog> page=query()
                .eq("user_id",userId)
                .orderByDesc("liked")
                .page(new Page<>(current,SystemConstants.DEFAULT_PAGE_SIZE));
        List<Blog> blogList = page.getRecords();
        return Result.ok(blogList);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        Long userId = UserHolder.getUser().getId();
        blog.setUserId(userId);
        // 保存探店笔记
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("新增笔记失败!");
        }
        //推送给粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", userId).list();
        for (Follow follow : follows) {
            //粉丝id
            Long uId = follow.getUserId();
            String key="feed:"+uId;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }

        // 返回id
        return Result.ok(userId);
    }

    @Override
    public Result queryBlogOfFollow(Long lastId, Integer offset) {
        Long userId = UserHolder.getUser().getId();
        String key=FEED_KEY+userId;
        //通过score滚动查询
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, lastId, offset, 2);
        if(typedTuples==null ||typedTuples.isEmpty()){
            return Result.ok();
        }
        //获取 blogID,minTime,offset
        List<Long> blogIds=new ArrayList<>(typedTuples.size());
        long minTime=0L;
        int os=1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            Long blogId = Long.valueOf(tuple.getValue());
            blogIds.add(blogId);
            long time= tuple.getScore().longValue();
            if(minTime==time){
                os++;
            }else{
                minTime=time;
                os=1;
            }
        }
        String join = StrUtil.join(",", blogIds);
        List<Blog> blogList = query().in("id", blogIds).last("ORDER BY Field(id," + join + ")").list();
        for (Blog blog : blogList) {
            //封装是否点赞
            queryIsLiked(blog);
            //封装发布blog的用户
            queryBlogUser(blog);
        }
        ScrollResult scrollResult=new ScrollResult();
        scrollResult.setList(blogList);
        scrollResult.setOffset(os);
        scrollResult.setMinTime(minTime);
        return Result.ok(scrollResult);
    }
}
