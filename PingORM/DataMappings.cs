using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace PingORM
{
    public class DataMappingSettings : SettingsBase<DataMappingSettings>
    {
        [ConfigurationProperty("tables", IsDefaultCollection = true)]
        [ConfigurationCollection(typeof(TableMappingSettingsCollection), AddItemName = "table")]
        public TableMappingSettingsCollection Tables
        {
            get { return this["tables"] as TableMappingSettingsCollection; }
            set { this["tables"] = value; }
        }
    }

    public class TableMappingSettings : ConfigurationElement
    {
        /// <summary>
        /// The name of the database key.
        /// </summary>
        [ConfigurationProperty("key", IsRequired = true)]
        public string Key
        {
            get { return (string)this["key"]; }
            set { this["key"] = value; }
        }

        /// <summary>
        /// The name of the database table.
        /// </summary>
        [ConfigurationProperty("tableName", IsRequired = true)]
        public string TableName
        {
            get { return (string)this["tableName"]; }
            set { this["tableName"] = value; }
        }

        /// <summary>
        /// The name of the entity to which this table is mapped.
        /// </summary>
        [ConfigurationProperty("entityName", IsRequired = true)]
        public string EntityName
        {
            get { return (string)this["entityName"]; }
            set { this["entityName"] = value; }
        }

        /// <summary>
        /// The name of the sequence that generates the id values for records in this table if there is one.
        /// </summary>
        [ConfigurationProperty("sequenceName", IsRequired = false)]
        public string SequenceName
        {
            get { return (string)this["sequenceName"]; }
            set { this["sequenceName"] = value; }
        }

        [ConfigurationProperty("columns", IsDefaultCollection = true)]
        [ConfigurationCollection(typeof(ColumnMappingSettingsCollection), AddItemName = "column")]
        public ColumnMappingSettingsCollection Columns
        {
            get { return this["columns"] as ColumnMappingSettingsCollection; }
            set { this["columns"] = value; }
        }
    }

    public class ColumnMappingSettings : ConfigurationElement
    {
        /// <summary>
        /// The name of the property that to which this column is mapped.
        /// </summary>
        [ConfigurationProperty("propertyName", IsRequired = true)]
        public string PropertyName
        {
            get { return (string)this["propertyName"]; }
            set { this["propertyName"] = value; }
        }

        /// <summary>
        /// The name of the database column.
        /// </summary>
        [ConfigurationProperty("columnName", IsRequired = true)]
        public string ColumnName
        {
            get { return (string)this["columnName"]; }
            set { this["columnName"] = value; }
        }

        /// <summary>
        /// Whether or not this column is part of the table's primary key.
        /// </summary>
        [ConfigurationProperty("isPrimaryKey", IsRequired = false)]
        public bool IsPrimaryKey
        {
            get { return (bool)this["isPrimaryKey"]; }
            set { this["isPrimaryKey"] = value; }
        }

        /// <summary>
        /// Whether or not this column is a partition key.
        /// </summary>
        [ConfigurationProperty("isPartitionKey", IsRequired = false)]
        public bool IsPartitionKey
        {
            get { return (bool)this["isPartitionKey"]; }
            set { this["isPartitionKey"] = value; }
        }

        /// <summary>
        /// The parent property name for nested properties.
        /// </summary>
        [ConfigurationProperty("parentPropertyName", IsRequired = false)]
        public string ParentPropertyName
        {
            get { return (string)this["parentPropertyName"]; }
            set { this["parentPropertyName"] = value; }
        }
    }

    public class TableMappingSettingsCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new TableMappingSettings();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((TableMappingSettings)element).TableName;
        }

        public TableMappingSettings this[int index]
        {
            get
            {
                return (TableMappingSettings)BaseGet(index);
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

        new public TableMappingSettings this[string name]
        {
            get
            {
                return (TableMappingSettings)BaseGet(name);
            }
        }
    }

    public class ColumnMappingSettingsCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new ColumnMappingSettings();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((ColumnMappingSettings)element).ColumnName;
        }

        public ColumnMappingSettings this[int index]
        {
            get
            {
                return (ColumnMappingSettings)BaseGet(index);
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

        new public ColumnMappingSettings this[string name]
        {
            get
            {
                return (ColumnMappingSettings)BaseGet(name);
            }
        }
    }
}
