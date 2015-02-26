using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PingORM.Redis.Configuration
{
    /// <summary>
    /// Specifies that a property is used as the field part of the key for a hash set.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class RedisValueAttribute : Attribute
    {
        /// <summary>
        /// Whether or not this is a sequence (used for IDs), which should be updated with a unique value when a new item is inserted.
        /// </summary>
        public bool IsSequence { get; set; }
    }
}
