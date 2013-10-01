using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using PingORM.Redis.Configuration;
using PingORM.Redis.Json;
using Sider;

namespace PingORM.Redis
{
    /// <summary>
    /// Class that handles mapping of redis entities.
    /// </summary>
    public class RedisEntityMapper
    {
        private static Dictionary<Type, RedisEntityMapping> _mappings = new Dictionary<Type, RedisEntityMapping>();

        /// <summary>
        /// Load the mapping attributes for the specified type.
        /// </summary>
        /// <param name="assembly"></param>
        static RedisEntityMapping LoadMapping(Type type)
        {
            // Check if this mapping has already been loaded.
            if (!_mappings.ContainsKey(type))
            {
                object[] attributes = type.GetCustomAttributes(typeof(RedisEntityAttribute), false);
                if (attributes.Length > 0)
                {
                    RedisEntityAttribute attribute = attributes[0] as RedisEntityAttribute;

                    if (attribute == null || String.IsNullOrEmpty(attribute.BaseKey))
                        throw new Exception(String.Format("Cannot load mappings for type [{0}].", type.FullName));

                    _mappings.Add(type, new RedisEntityMapping(attribute, type));
                }
            }

            return _mappings[type];
        }

        /// <summary>
        /// Gets the redis key for the specified entity based upon the redis entity mappings.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static string GetKey<T>(T entity = null) where T : class
        {
            // Make sure the mappings have been loaded for this type.
            RedisEntityMapping mapping = LoadMapping(typeof(T));

            // Start with the primary key.
            StringBuilder key = new StringBuilder(mapping.PrimaryKey);

            // Append a secondary key if there is one.
            if (!String.IsNullOrWhiteSpace(mapping.SecondaryKey))
                key.Append(":").Append(mapping.SecondaryKey);

            // Append the value of each key property.
            if (entity != null)
            {
                foreach (RedisKeyMapping keyProperty in mapping.KeyProperties.OrderBy(k => k.Order))
                    key.Append(":").Append(keyProperty.PropertyInfo.GetValue(entity).ToString());
            }

            return key.ToString();
        }

        /// <summary>
        /// Gets the redis field for the specified entity based upon the redis entity mappings.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static string GetField<T>(T entity)
        {
            return LoadMapping(typeof(T)).FieldProperty.PropertyInfo.GetValue(entity).ToString();
        }

        /// <summary>
        /// Inserts an entity into Redis.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static ENTITY Insert<ENTITY>(IRedisClient<string> client, ENTITY entity) where ENTITY : class
        {
            // Make sure the mappings have been loaded for this type.
            RedisEntityMapping mapping = LoadMapping(typeof(ENTITY));

            // If the field property is a sequence, get the next sequence value.
            if (mapping.FieldProperty != null && mapping.FieldProperty.IsSequence)
                mapping.FieldProperty.PropertyInfo.SetValue(entity, (int)client.Incr(String.Format("seq:{0}", typeof(ENTITY).Name)));

            // Set the value in redis.
            client.HSet(GetKey(entity), GetField(entity), JsonSerializer.SerializeObject(entity));

            return entity;
        }

        /// <summary>
        /// Loads all of the entities of a specified type.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="client"></param>
        /// <returns></returns>
        public static List<ENTITY> GetAll<ENTITY>(IRedisClient<string> client) where ENTITY : class
        {
            // Make sure the mappings have been loaded for this type.
            RedisEntityMapping mapping = LoadMapping(typeof(ENTITY));

            List<ENTITY> list = new List<ENTITY>();
            foreach (KeyValuePair<string, string> item in client.HGetAll(GetKey<ENTITY>()))
                list.Add(JsonSerializer.DeserializeObject(item.Value, typeof(ENTITY)) as ENTITY);

            return list;
        }

        /// <summary>
        /// Increments the value of the specified field by the specified amount.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="client"></param>
        /// <param name="entity"></param>
        /// <param name="field"></param>
        /// <param name="amount"></param>
        public static void Increment<ENTITY>(IRedisClient<string> client, ENTITY entity, object field, double amount) where ENTITY : class
        {
            client.HIncrByFloat(GetKey<ENTITY>(), field.ToString(), amount);
        }

        /// <summary>
        /// Represents a mapping from a class to a redis entity.
        /// </summary>
        class RedisEntityMapping
        {
            public string PrimaryKey { get; set; }
            public string SecondaryKey { get; set; }
            public List<RedisKeyMapping> KeyProperties { get; set; }
            public RedisFieldMapping FieldProperty { get; set; }

            public RedisEntityMapping(RedisEntityAttribute attribute, Type type)
            {
                this.PrimaryKey = attribute.BaseKey;
                this.SecondaryKey = attribute.SecondaryKey;
                this.KeyProperties = new List<RedisKeyMapping>();

                // Load all of the redis key/field property attributes.
                foreach (PropertyInfo property in type.GetProperties())
                {
                    object[] propertyAttrs = property.GetCustomAttributes(typeof(RedisKeyAttribute), true);
                    if (propertyAttrs.Length > 0)
                        this.KeyProperties.Add(new RedisKeyMapping(property, propertyAttrs[0] as RedisKeyAttribute));

                    propertyAttrs = property.GetCustomAttributes(typeof(RedisFieldAttribute), true);
                    if (propertyAttrs.Length > 0)
                        this.FieldProperty = new RedisFieldMapping(property, propertyAttrs[0] as RedisFieldAttribute);
                }
            }
        }

        /// <summary>
        /// Represents a mapping from a property to part of a redis key.
        /// </summary>
        class RedisKeyMapping
        {
            public PropertyInfo PropertyInfo { get; set; }
            public int Order { get; set; }

            public RedisKeyMapping(PropertyInfo property, RedisKeyAttribute attribute)
            {
                this.PropertyInfo = property;
                this.Order = attribute.Order;
            }
        }

        /// <summary>
        /// Represents a mapping from a property to a redis hash set field.
        /// </summary>
        class RedisFieldMapping
        {
            public PropertyInfo PropertyInfo { get; set; }
            public bool IsSequence { get; set; }

            public RedisFieldMapping(PropertyInfo property, RedisFieldAttribute attribute)
            {
                this.PropertyInfo = property;
                this.IsSequence = attribute.IsSequence;
            }
        }
    }
}
