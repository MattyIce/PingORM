using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PingORM.Redis.Configuration
{
    /// <summary>
    /// Specifies that a property is part of the key for this object.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class RedisKeyAttribute : Attribute
    {
        /// <summary>
        /// The order in which this part of the key should appear.
        /// </summary>
        public int Order { get; set; }
    }
}
