package com.cj.flink.utils;


import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class RedisUtil implements Closeable, Serializable {
    private static final Logger log = LoggerFactory.getLogger(RedisUtil.class);
    private transient JedisCluster jedisCluster;

    public RedisUtil() {
        try {
            JedisClusterConfig jedisClusterConfig = JedisClusterConfig.getClusterConfig();
            GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
            genericObjectPoolConfig.setMaxIdle(jedisClusterConfig.getMaxIdle());
            genericObjectPoolConfig.setMaxTotal(jedisClusterConfig.getMaxTotal());
            genericObjectPoolConfig.setMinIdle(jedisClusterConfig.getMinIdle());

            JedisCluster jedisCluster = new JedisCluster(jedisClusterConfig.getNodes(), jedisClusterConfig.getConnectionTimeout(),
                    jedisClusterConfig.getMaxRedirections(), genericObjectPoolConfig);
            Objects.requireNonNull(jedisCluster, "Jedis cluster can not be null");

            this.jedisCluster = jedisCluster;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    public String set(String key, String value, String nxxx, String expx, long time) {
        String re="";
        try {
            re=jedisCluster.set(key, value,nxxx,expx,time);
        } catch (JedisConnectionException e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }
        if(re==null){
            re="-1";
        }
        return  re;
    }
    public void pfadd(final String key, int seconds, final String... element) {
        try {
            jedisCluster.pfadd(key, element);
            jedisCluster.expire(key, seconds);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }
    }

    public void pfadd(final String key, int seconds, List<String> elements) {
        try {
            pfadd(key, seconds, elements.toArray(new String[elements.size()]));
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }
    }
    public void pfadd(final String key, int seconds, Set<String> elements) {
        try {
            pfadd(key, seconds, elements.toArray(new String[elements.size()]));
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }
    }
    public long pfcount(final String key) {
        try {
            return jedisCluster.pfcount(key);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public void sadd(final String key, int seconds, final String... element) {
        try {
            jedisCluster.sadd(key, element);
            jedisCluster.expire(key, seconds);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }
    }

    public void sadd(final String key, int seconds, List<String> elements) {
        try {
            sadd(key, seconds, elements.toArray(new String[elements.size()]));
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }

    }

    public long scard(String key) {
        try {
            return jedisCluster.scard(key);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public Long incrBy(final String key, long integer, int seconds) {
        try {
            long value = jedisCluster.incrBy(key, integer);
            jedisCluster.expire(key, seconds);
            return value;
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public String get(String key) {
        try {
            return jedisCluster.get(key);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return null;
        }
    }

    public boolean exists(String key) {
        try {
            return jedisCluster.exists(key);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return false;
        }
    }

    public String set(String key, String value) {
        try {
            return jedisCluster.set(key, value);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return null;
        }
    }

    public String set(String key, String value, int seconds) {
        try {
            String re = jedisCluster.set(key, value);
            jedisCluster.expire(key, seconds);
            return re;
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return null;
        }
    }

    public void hmset(String key, Map<String, String> hash, int seconds) {
        try {
            jedisCluster.hmset(key, hash);
            jedisCluster.expire(key, seconds);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }
    }

    public List<String> hmget(String key, String... fields) {
        try {
            return jedisCluster.hmget(key, fields);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return null;
        }
    }

    public void setex(String key, int time, String value) {
        try {
            jedisCluster.setex(key, time, value);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }
    }

    public Long hlen(String key) {
        try {
            return jedisCluster.hlen(key);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public void hset(String key, String field, String value) {
        try {
            jedisCluster.hset(key, field, value);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }
    }

    public void hset(String key, String field, String value, int seconds) {
        try {
            jedisCluster.hset(key, field, value);
            jedisCluster.expire(key, seconds);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }
    }

    public void hdel(String key, String field) {
        try {
            jedisCluster.hdel(key, field);
        } catch (
                Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
        }

    }

    public String hget(String key, String field) {
        try {
            return jedisCluster.hget(key, field);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return null;
        }
    }

    public Map<String, String> hgetAll(String key) {
        try {
            return jedisCluster.hgetAll(key);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return null;
        }
    }

    public long hincrBy(String key, String field, long value) {
        try {
            return jedisCluster.hincrBy(key, field, value);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public long hincrBy(String key, String field, long value, int seconds) {
        try {
            long re = jedisCluster.hincrBy(key, field, value);
            jedisCluster.expire(key, seconds);
            return re;
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public double hincrByFloat(String key, String field, double value) {
        try {
            return jedisCluster.hincrByFloat(key, field, value);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1D;
        }
    }

    public double hincrByFloat(String key, String field, double value, int seconds) {
        try {
            double re = jedisCluster.hincrByFloat(key, field, value);
            jedisCluster.expire(key, seconds);
            return re;
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1D;
        }
    }

    public Long del(String key) {
        try {
            return jedisCluster.del(key);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public boolean hexists(String key, String field) {
        try {
            return jedisCluster.hexists(key, field);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return false;
        }
    }

    public long pfadd(String key, String value, int seconds) {
        try {
            long re = jedisCluster.pfadd(key, value);
            jedisCluster.expire(key, seconds);
            return re;
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public long zadd(String key, double score, String member, int seconds) {
        try {
            long re = jedisCluster.zadd(key, score, member);
            jedisCluster.expire(key, seconds);
            return re;
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public long zadd(String key, double score, String member) {
        try {
            return jedisCluster.zadd(key, score, member);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public Set<String> smembers(String key) {
        try {
            return jedisCluster.smembers(key);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return null;
        }
    }

    /**
     * 返回集合大小
     */
    public long sadd(String key, String value) {
        try {
            return jedisCluster.sadd(key, value);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1;
        }
    }

    public long srem(String key, List<String> elements) {
        try {
            return jedisCluster.srem(key, elements.toArray(new String[elements.size()]));
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    /**
     * 返回集合大小
     */
    public long sadd(String key, String value, int seconds) {
        try {
            long re = jedisCluster.sadd(key, value);
            jedisCluster.expire(key, seconds);
            return re;
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public long sadd(String key, String... values) {
        try {
            return jedisCluster.sadd(key, values);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public long sadd(String key, List<String> elements) {
        try {
            return jedisCluster.sadd(key, elements.toArray(new String[elements.size()]));
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public long sadd(String key, List<String> elements, int seconds) {
        try {
            long re = jedisCluster.sadd(key, elements.toArray(new String[elements.size()]));
            jedisCluster.expire(key, seconds);
            return re;
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    public double zincrby(String key, double score, String member, int seconds) {
        try {
            double re = jedisCluster.zincrby(key, score, member);
            jedisCluster.expire(key, seconds);
            return re;
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1D;
        }
    }

    public long incrBy(String key, long integer) {
        try {
            return jedisCluster.incrBy(key, integer);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }


    public double incrByFloat(String key, double score, int seconds) {
        try {
            double re = jedisCluster.incrByFloat(key, score);
            jedisCluster.expire(key, seconds);
            return re;
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1D;
        }
    }

    public double incrByFloat(String key, double score) {
        try {
            return jedisCluster.incrByFloat(key, score);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1D;
        }
    }

    public Long zcount(String key, String min, String max) {
        try {
            return jedisCluster.zcount(key, min, max);
        } catch (Exception e) {
            log.error("redis 命令执行出错， key {} error message {}", key, e);
            return -1L;
        }
    }

    @Override
    public void close() throws IOException {
        this.jedisCluster.close();
    }
}
