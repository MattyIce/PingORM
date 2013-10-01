using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PingORM.Redis.Configuration
{
    /// <summary>
    /// Specifies that a class is a redis entity.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class RedisEntityAttribute : Attribute
    {
        /// <summary>
        /// The primary key name for all objects of this type, keys look like: [primary]:[secondary].
        /// </summary>
        public string BaseKey { get; set; }

        /// <summary>
        /// The secondary key name for all objects of this type, keys look like: [primary]:[secondary].
        /// The secondary key can also be added to a property.
        /// </summary>
        public string SecondaryKey { get; set; }
    }
}
