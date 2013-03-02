using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PingORM
{
    /// <summary>
    /// Specifies that a class is an adapter for a specific type of data entity.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class EntityAdapterAttribute : Attribute
    {
        /// <summary>
        /// The type of entity that this adapter handles.
        /// </summary>
        public Type EntityType { get; set; }
    }
}
