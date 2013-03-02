using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PingORM
{
    /// <summary>
    /// Specifies that a class is a data entity.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class DataEntityAttribute : Attribute
    {
        /// <summary>
        /// The data connection/session key to use for this type of data entity.
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// The name of the table that this entity maps to.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// The name of the DB sequence that generates the Id for entities of this type.
        /// </summary>
        public string SequenceName { get; set; }
    }
}
