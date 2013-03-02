using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace PingORM
{
    /// <summary>
    /// Configuration settings for database connections.
    /// </summary>
    public class ConnectionSettings : ConfigurationSection
    {
        /// <summary>
        /// The advertiser settings for this game.
        /// </summary>
        public static ConnectionSettings Current
        {
            get
            {
                if (_current == null)
                    _current = ConfigurationManager.GetSection("connectionSettings") as ConnectionSettings;

                return _current;
            }
        }
        private static ConnectionSettings _current;

        public ConnectionElement GetValue(string key) { return Connections[key]; }

        [ConfigurationProperty("connections", IsDefaultCollection = false)]
        [ConfigurationCollection(typeof(ConnectionCollection), AddItemName = "connection")]
        public ConnectionCollection Connections
        {
            get
            {
                return this["connections"] as ConnectionCollection;
            }
            set { this["connections"] = value; }
        }
    }

    public class ConnectionElement : ConfigurationElement
    {
        /// <summary>
        /// The name/key of this connection.
        /// </summary>
        [ConfigurationProperty("key", IsRequired = true)]
        public string Key
        {
            get { return (string)this["key"]; }
            set { this["key"] = value; }
        }

        /// <summary>
        /// The connection string for this connection.
        /// </summary>
        [ConfigurationProperty("connectionString", IsRequired = true)]
        public string ConnectionString
        {
            get { return (string)this["connectionString"]; }
            set { this["connectionString"] = value; }
        }

        /// <summary>
        /// The name of the assembly containing the data entity and mappings.
        /// </summary>
        [ConfigurationProperty("mappingsAssembly", IsRequired = true)]
        public string MappingsAssembly
        {
            get { return (string)this["mappingsAssembly"]; }
            set { this["mappingsAssembly"] = value; }
        }

        /// <summary>
        /// The name of the assembly containing the custom data adapters.
        /// </summary>
        [ConfigurationProperty("adaptersAssembly", IsRequired = false)]
        public string AdaptersAssembly
        {
            get { return (string)this["adaptersAssembly"]; }
            set { this["adaptersAssembly"] = value; }
        }
    }

    public class ConnectionCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new ConnectionElement();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((ConnectionElement)element).Key;
        }

        public ConnectionElement this[int index]
        {
            get
            {
                return (ConnectionElement)BaseGet(index);
            }
            set
            {
                if (BaseGet(index) != null)
                {
                    BaseRemoveAt(index);
                }
                BaseAdd(index, value);
            }
        }

        new public ConnectionElement this[string name]
        {
            get
            {
                return (ConnectionElement)BaseGet(name);
            }
        }
    }
}
