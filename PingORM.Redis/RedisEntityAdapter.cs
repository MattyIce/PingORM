using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sider;

namespace PingORM.Redis
{
    public interface IRedisEntityAdapter
    {
        ENTITY Insert<ENTITY>(ENTITY entity) where ENTITY : class;
        List<ENTITY> GetAll<ENTITY>() where ENTITY : class;
        double Increment(string baseKey, object key, object field, double amount);
    }

    public class RedisEntityAdapter : IRedisEntityAdapter
    {
        /// <summary>
        /// The redis client to use.
        /// </summary>
        public IRedisClient RedisClient { get; set; }

        /// <summary>
        /// Create a new instance of the RedisEntityAdapter with the specified Redis client.
        /// </summary>
        /// <param name="client"></param>
        public RedisEntityAdapter(IRedisClient client) { this.RedisClient = client; }

        /// <summary>
        /// Inserts a new entity into redis.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        public ENTITY Insert<ENTITY>(ENTITY entity) where ENTITY : class
        {
            return RedisEntityMapper.Insert(RedisClient, entity);
        }

        /// <summary>
        /// Gets all of the items of the specified entity type.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <returns></returns>
        public List<ENTITY> GetAll<ENTITY>() where ENTITY : class
        {
            return RedisEntityMapper.GetAll<ENTITY>(RedisClient);
        }

        /// <summary>
        /// Increment a key by a certain amount.
        /// </summary>
        /// <param name="baseKey"></param>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="amount"></param>
        /// <returns></returns>
        public double Increment(string baseKey, object key, object field, double amount)
        {
            return RedisClient.HIncrByFloat(String.Format("{0}:{1}", baseKey, key), field.ToString(), amount);
        }

        public string[] GetItems(string baseKey, object key, object[] fields)
        {
            return RedisClient.HMGet(String.Format("{0}:{1}", baseKey, key), fields.Select(f => f.ToString()).ToArray());
        }
    }
}
