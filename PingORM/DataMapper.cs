using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Npgsql;
using System.Reflection;
using System.Data;
using NpgsqlTypes;
using PingORM.Configuration;

namespace PingORM
{
    public static class DataMapper
    {
        static log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        static log4net.ILog SqlLog = log4net.LogManager.GetLogger("SQL");
        private static Dictionary<Type, TableMapping> _mappings = new Dictionary<Type, TableMapping>();

        static DataMapper()
        {
            // Find any XML mappings.
            if (DataMappingSettings.Current != null)
            {
                foreach (TableMappingSettings tableMapping in DataMappingSettings.Current.Tables)
                {
                    Type type = Type.GetType(tableMapping.EntityName);

                    if (_mappings.ContainsKey(type))
                        continue;

                    TableMapping mapping = new TableMapping(tableMapping);
                    _mappings.Add(type, mapping);
                }
            }
        }

        /// <summary>
        /// Load the mappings attributes for the specified type.
        /// </summary>
        /// <param name="assembly"></param>
        static void LoadMappings(Type type)
        {
            // Check if this mapping has already been loaded.
            if (_mappings.ContainsKey(type))
                return;

            object[] attributes = type.GetCustomAttributes(typeof(DataEntityAttribute), false);
            if (attributes.Length > 0)
            {
                DataEntityAttribute attribute = attributes[0] as DataEntityAttribute;

                if (attribute == null || String.IsNullOrEmpty(attribute.TableName))
                    throw new Exception(String.Format("Cannot load mappings for type [{0}].", type.FullName));

                TableMapping mapping = new TableMapping(attribute, type);
                _mappings.Add(type, mapping);
            }
        }

        /// <summary>
        /// Selects a record of the specified type from the database by its Id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="session"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public static T Get<T>(ISession session, object id) where T : class, new()
        {
            return Get<T>(session, id, null);
        }

        /// <summary>
        /// Selects a record of the specified type from a partitioned table in the database by its id and timestamp.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="session"></param>
        /// <param name="id"></param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        public static T Get<T>(ISession session, object id, object partitionKey) where T : class, new()
        {
            return Get(typeof(T), session, id, partitionKey) as T;
        }

        /// <summary>
        /// Selects a record of the specified type from a partitioned table in the database by its id and timestamp.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="session"></param>
        /// <param name="id"></param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        public static object Get(Type type, ISession session, object id, object partitionKey)
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(type);

            try
            {
                TableMapping mapping = _mappings[type];

                // Make sure that the table is not partitioned.
                if (mapping.IsPartitioned && partitionKey == null)
                    throw new Exception("Cannot select an object from a partitioned table without a partition key.");

                NpgsqlCommand selectCommand = new NpgsqlCommand(mapping.GetExpression, (NpgsqlConnection)session.Connection);

                // Get the primary key column(s).
                List<ColumnMapping> idColumns = mapping.Columns.Where(c => c.IsPk).ToList();

                // Check if we have multiple PKs or just one.
                if (idColumns.Count == 1)
                    selectCommand.Parameters.Add(new NpgsqlParameter("p_id0", idColumns[0].DbType)).Value = id;
                else
                {
                    int i = 0;

                    // If we have multiple PKs then add a parameter for each of them.
                    foreach (ColumnMapping idColumn in idColumns)
                        selectCommand.Parameters.Add(new NpgsqlParameter(String.Format("p_id{0}", i++), idColumn.DbType)).Value = idColumn.PropertyInfo.GetValue(id, null);
                }

                // Add the partition key timestamp parameter.
                if (mapping.IsPartitioned)
                {
                    ColumnMapping idColumn = mapping.Columns.FirstOrDefault(c => c.IsPartitionKey);
                    selectCommand.Parameters.Add(new NpgsqlParameter("p_ts", idColumn.DbType)).Value = partitionKey;
                }

                LogCommand(selectCommand);
                using (NpgsqlDataReader reader = selectCommand.ExecuteReader())
                {
                    if (reader.Read())
                        return FromDb(type, reader);
                }
            }
            catch (Exception ex) { Log.Error("DataMapper.Get threw an exception.", ex); }

            return null;
        }

        /// <summary>
        /// Gets the identifier of the specified entity based upon the Primary Key defined in the mapping.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static object GetId<T>(T entity)
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(typeof(T));

            TableMapping mapping = _mappings[typeof(T)];
            
            // Get the primary key column(s).
            List<ColumnMapping> idColumns = mapping.Columns.Where(c => c.IsPk).ToList();

            // Check if we have multiple PKs or just one.
            if (idColumns.Count == 1)
                return idColumns[0].PropertyInfo.GetValue(entity);
            else
                throw new NotSupportedException("Getting the Id of a multi-column PK object is not supported in this version.");
        }

        /// <summary>
        /// Reads an object of the specified type from the specified data reader.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="reader"></param>
        /// <returns></returns>
        private static object FromDb(Type type, NpgsqlDataReader reader)
        {
            TableMapping mapping = _mappings[type];
            object record = Activator.CreateInstance(type);

            foreach (ColumnMapping column in mapping.Columns)
            {
                try
                {
                    object value = reader[column.ColumnName];

                    if (value == System.DBNull.Value)
                        continue;

                    column.SetValue(record, value);
                }
                catch (Exception ex) { Log.Error(String.Format("DataMapper.FromDb threw an exception getting column [{0}].", column.ColumnName), ex); }
            }

            return record;
        }

        /// <summary>
        /// Executes the select statement specified by the provided query builder.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="session"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static IEnumerator<T> Select<T>(ISession session, QueryBuilder<T> query) where T : class, new()
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(typeof(T));

            NpgsqlCommand selectCommand = new NpgsqlCommand(query.ToString(), (NpgsqlConnection)session.Connection);

            if (query.Parameters != null)
            {
                // Add the parameters from the query builder.
                foreach (KeyValuePair<string, object> parameter in query.Parameters)
                    selectCommand.Parameters.Add(new NpgsqlParameter(parameter.Key, ColumnMapping.GetDbType(parameter.Value.GetType()))).Value = parameter.Value;
            }

            List<T> results = new List<T>();

            LogCommand(selectCommand);
            using (NpgsqlDataReader reader = selectCommand.ExecuteReader())
            {
                while (reader.Read())
                    yield return FromDb(typeof(T), reader) as T;
            }
        }

        /// <summary>
        /// Executes the select statement specified by the provided query builder.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="session"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static object SelectScalar<T>(ISession session, QueryBuilder<T> query) where T : class, new()
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(typeof(T));

            try
            {
                NpgsqlCommand selectCommand = new NpgsqlCommand(query.ToString(), (NpgsqlConnection)session.Connection);

                if (query.Parameters != null)
                {
                    // Add the parameters from the query builder.
                    foreach (KeyValuePair<string, object> parameter in query.Parameters)
                        selectCommand.Parameters.Add(new NpgsqlParameter(parameter.Key, ColumnMapping.GetDbType(parameter.Value.GetType()))).Value = parameter.Value;
                }

                LogCommand(selectCommand);
                return selectCommand.ExecuteScalar();
            }
            catch (Exception ex) { Log.Error("DataMapper.Select threw an exception.", ex); }

            return null;
        }

        /// <summary>
        /// Inserts a new entity into the database.
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static int Insert(ISession session, object entity)
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(entity.GetType());

            try
            {
                TableMapping mapping = _mappings[entity.GetType()];
                NpgsqlCommand insertCommand = new NpgsqlCommand(mapping.InsertExpression, (NpgsqlConnection)session.Connection);

                int i = 0;
                foreach (ColumnMapping column in mapping.Columns)
                {
                    if (column.IsPk && !String.IsNullOrEmpty(mapping.SequenceName))
                    {
                        NpgsqlCommand seqCommand = new NpgsqlCommand(String.Format("select nextval('\"{0}\"')", mapping.SequenceName), (NpgsqlConnection)session.Connection);
                        LogCommand(seqCommand);
                        column.PropertyInfo.SetValue(entity, Convert.ChangeType(seqCommand.ExecuteScalar(), column.PropertyType), null);
                    }

                    insertCommand.Parameters.Add(new NpgsqlParameter(String.Format("p{0}", i++), column.DbType)).Value = column.GetValue(entity);
                }

                LogCommand(insertCommand);
                return insertCommand.ExecuteNonQuery();
            }
            catch (Exception ex) { Log.Error("DataMapper.Insert threw an exception.", ex); }

            return 0;
        }

        /// <summary>
        /// Executes a non-select query using the specified query builder. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="session"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static int NonQuery<T>(ISession session, QueryBuilder<T> query) where T : class, new()
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(typeof(T));

            try
            {
                NpgsqlCommand command = new NpgsqlCommand(query.ToString(), (NpgsqlConnection)session.Connection);

                if (query.Parameters != null)
                {
                    // Add the parameters from the query builder.
                    foreach (KeyValuePair<string, object> parameter in query.Parameters)
                        command.Parameters.Add(new NpgsqlParameter(parameter.Key, ColumnMapping.GetDbType(parameter.Value.GetType()))).Value = parameter.Value;
                }

                LogCommand(command);
                return command.ExecuteNonQuery();
            }
            catch (Exception ex) { Log.Error("DataMapper.NonQuery threw an exception.", ex); }

            return 0;
        }

        /// <summary>
        /// Updates changes to an existing entity in the database.
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static int Update(ISession session, object entity)
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(entity.GetType());

            try
            {
                TableMapping mapping = _mappings[entity.GetType()];
                NpgsqlCommand updateCommand = new NpgsqlCommand(mapping.UpdateExpression, (NpgsqlConnection)session.Connection);

                int i = 0;
                foreach (ColumnMapping column in mapping.Columns)
                    updateCommand.Parameters.Add(new NpgsqlParameter(String.Format("p{0}", i++), column.DbType)).Value = column.GetValue(entity);

                LogCommand(updateCommand);
                return updateCommand.ExecuteNonQuery();
            }
            catch (Exception ex) { Log.Error("DataMapper.Update threw an exception.", ex); }

            return 0;
        }

        /// <summary>
        /// Deletes the specified item from the database.
        /// </summary>
        /// <param name="session"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static int Delete(ISession session, object entity)
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(entity.GetType());

            try
            {
                TableMapping mapping = _mappings[entity.GetType()];
                NpgsqlCommand deleteCommand = new NpgsqlCommand(mapping.DeleteExpression, (NpgsqlConnection)session.Connection);

                int i = 0;
                foreach (ColumnMapping column in mapping.Columns.Where(c => c.IsPk))
                    deleteCommand.Parameters.Add(new NpgsqlParameter(String.Format("p{0}", i++), column.DbType)).Value = column.GetValue(entity);

                LogCommand(deleteCommand);
                return deleteCommand.ExecuteNonQuery();
            }
            catch (Exception ex) { Log.Error("DataMapper.Delete threw an exception.", ex); }

            return 0;
        }

        /// <summary>
        /// Run an ad-hoc query.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        public static List<Dictionary<string, object>> CustomQuery(string key, string sql, Dictionary<string, object> parameters)
        {
            return CustomQuery(SessionFactory.GetCurrentSession(key), sql, parameters);
        }

        /// <summary>
        /// Run an ad-hoc query.
        /// </summary>
        /// <param name="session"></param>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        public static List<Dictionary<string, object>> CustomQuery(ISession session, string sql, Dictionary<string, object> parameters)
        {
            List<Dictionary<string, object>> results = new List<Dictionary<string, object>>();

            try
            {
                NpgsqlCommand command = new NpgsqlCommand(sql, (NpgsqlConnection)session.Connection);

                if (parameters != null)
                {
                    foreach (KeyValuePair<string, object> kvp in parameters)
                        command.Parameters.Add(new NpgsqlParameter(kvp.Key, ColumnMapping.GetDbType(kvp.Value.GetType()))).Value = kvp.Value;
                }

                LogCommand(command);
                using (NpgsqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Dictionary<string, object> result = new Dictionary<string, object>();

                        for (int i = 0; i < reader.FieldCount; i++)
                            result.Add(reader.GetName(i), reader[i]);

                        results.Add(result);                        
                    }
                }
            }
            catch (Exception ex) { Log.Error("DataMapper.Query threw an exception.", ex); }

            return results;
        }

        /// <summary>
        /// Logs a sql command to the SQL log4net log.
        /// </summary>
        /// <param name="command"></param>
        public static void LogCommand(NpgsqlCommand command)
        {
            if (SqlLog.IsDebugEnabled)
            {
                StringBuilder sb = new StringBuilder(command.CommandText);

                foreach (NpgsqlParameter param in command.Parameters)
                    sb.Append(String.Format(" :{0} = {1} [Type: {2}],", param.ParameterName, param.Value, param.NpgsqlDbType));

                SqlLog.Debug(sb.ToString());
            }
        }

        /// <summary>
        /// Gets the DB mapping information for a type.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static TableMapping GetTableMapping(Type type)
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(type);

            return _mappings[type];
        }

        /// <summary>
        /// Gets the column mapping for a property of the specified type.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static ColumnMapping GetColumnMapping(Type type, string propertyName)
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(type);

            return _mappings[type].Columns.FirstOrDefault(c => c.PropertyName == propertyName);
        }

        /// <summary>
        /// Gets the column name for a property of the specified type.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static string GetColumnName(Type type, string propertyName)
        {
            ColumnMapping column = GetColumnMapping(type, propertyName);
            return (column == null) ? null : column.ColumnName;
        }
    }

    /// <summary>
    /// Represents a mapping between a code entity and a DB table.
    /// </summary>
    public class TableMapping
    {
        public string Key { get; set; }
        public string TableName { get; set; }
        public string SequenceName { get; set; }
        public List<ColumnMapping> Columns { get; set; }
        public ColumnMapping PartitionKey { get { return Columns.FirstOrDefault(c => c.IsPartitionKey); } }
        public bool IsPartitioned { get { return PartitionKey != null; } }
        public string InsertExpression { get; set; }
        public string DeleteExpression { get; set; }
        public string UpdateExpression { get; set; }
        public string SelectExpression { get; set; }
        public string GetExpression { get; set; }

        public TableMapping()
        {
            this.Columns = new List<ColumnMapping>();
        }

        public TableMapping(DataEntityAttribute tableAttribute, Type type) : this()
        {
            this.Key = tableAttribute.Key;
            this.TableName = tableAttribute.TableName;
            this.SequenceName = tableAttribute.SequenceName;

            this.LoadColumns(type, null);
            this.Initialize();
        }

        public TableMapping(TableMappingSettings tableMapping) : this()
        {
            this.Key = tableMapping.Key;
            this.TableName = tableMapping.TableName;
            this.SequenceName = tableMapping.SequenceName;

            this.LoadColumns(tableMapping);
            this.Initialize();
        }

        /// <summary>
        /// Loads the column attributes attached to the specified type's properties.
        /// </summary>
        /// <param name="mapping"></param>
        /// <param name="type"></param>
        protected void LoadColumns(Type type, PropertyInfo parentProperty)
        {
            foreach (PropertyInfo property in type.GetProperties())
            {
                object[] propertyAttrs = property.GetCustomAttributes(typeof(ColumnAttribute), true);
                if (propertyAttrs.Length > 0)
                {
                    ColumnAttribute columnAttr = propertyAttrs[0] as ColumnAttribute;

                    if (columnAttr.IsNestedType)
                        this.LoadColumns(property.PropertyType, property);
                    else
                        this.Columns.Add(new ColumnMapping(property, columnAttr, parentProperty));
                }
            }
        }

        /// <summary>
        /// Loads the column attributes attached to the specified type's properties.
        /// </summary>
        /// <param name="mapping"></param>
        /// <param name="type"></param>
        protected void LoadColumns(TableMappingSettings tableMapping)
        {
            Type type = Type.GetType(tableMapping.EntityName);
            foreach (ColumnMappingSettings columnMapping in tableMapping.Columns)
                this.Columns.Add(new ColumnMapping(columnMapping, type));
        }

        public void Initialize()
        {
            GenerateInsertExpression();
            GenerateDeleteExpression();
            GenerateUpdateExpression();
            GenerateSelectExpression();
            GenerateGetExpression();
        }

        public void GenerateInsertExpression()
        {
            StringBuilder columnStr = new StringBuilder();
            StringBuilder valueStr = new StringBuilder();

            int i = 0;
            foreach (ColumnMapping column in Columns)
            {
                columnStr.Append(String.Format("\"{0}\",", column.ColumnName));
                valueStr.Append(String.Format(":p{0},", i++));
            }

            this.InsertExpression = String.Format("INSERT INTO \"{0}\" ({1}) VALUES ({2});", this.TableName,
                columnStr.ToString().TrimEnd(new char[] { ',' }), valueStr.ToString().TrimEnd(new char[] { ',' }));
        }

        public void GenerateDeleteExpression()
        {
            StringBuilder whereStr = new StringBuilder();

            int i = 0;
            foreach (ColumnMapping column in Columns.Where(c => c.IsPk))
            {
                if (whereStr.Length > 0)
                    whereStr.Append(" AND ");

                whereStr.Append(String.Format("\"{0}\" = :p{1}", column.ColumnName, i++));
            }

            this.DeleteExpression = String.Format("DELETE FROM \"{0}\" WHERE {1};", this.TableName, whereStr.ToString());
        }

        public void GenerateUpdateExpression()
        {
            StringBuilder columnStr = new StringBuilder();
            StringBuilder whereStr = new StringBuilder();

            int i = 0;
            foreach (ColumnMapping column in Columns)
            {
                if (column.IsPk || column.IsPartitionKey)
                {
                    if (whereStr.Length > 0)
                        whereStr.Append(" AND ");

                    whereStr.Append(String.Format("\"{0}\" = :p{1}", column.ColumnName, i++));
                }
                else
                    columnStr.Append(String.Format("\"{0}\" = :p{1},", column.ColumnName, i++));
            }

            this.UpdateExpression = String.Format("UPDATE \"{0}\" SET {1} WHERE {2};", this.TableName,
                columnStr.ToString().TrimEnd(new char[] { ',' }), whereStr.ToString());
        }

        public void GenerateSelectExpression()
        {
            StringBuilder columnStr = new StringBuilder();
            StringBuilder whereStr = new StringBuilder();

            foreach (ColumnMapping column in Columns)
                columnStr.Append(String.Format("\"{0}\",", column.ColumnName));

            this.SelectExpression = String.Format("SELECT {0} FROM \"{1}\"", columnStr.ToString().TrimEnd(new char[] { ',' }), this.TableName);
        }

        public void GenerateGetExpression()
        {
            StringBuilder whereStr = new StringBuilder();

            int i = 0;
            foreach (ColumnMapping column in Columns)
            {
                if (column.IsPk || column.IsPartitionKey)
                {
                    if (whereStr.Length > 0)
                        whereStr.Append(" AND ");

                    whereStr.Append(String.Format("\"{0}\" = :p_{1}", column.ColumnName, column.IsPk ? String.Format("id{0}", i++) : "ts"));
                }
            }

            this.GetExpression = String.Format("{0} WHERE {1}", this.SelectExpression, whereStr.ToString());
        }
    }

    /// <summary>
    /// Represents a mapping between a code entity property and a DB table column.
    /// </summary>
    public class ColumnMapping
    {
        static log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public PropertyInfo PropertyInfo { get; set; }
        public string PropertyName { get; set; }
        public string ColumnName { get; set; }
        public Type PropertyType { get; set; }
        public NpgsqlDbType DbType { get; set; }
        public bool IsPk { get; set; }
        public bool IsPartitionKey { get; set; }
        public bool IsAutoGenerated { get; set; }
        public PropertyInfo ParentPropertyInfo { get; set; }

        public ColumnMapping(PropertyInfo property, ColumnAttribute attr)
        {
            this.PropertyInfo = property;
            this.PropertyType = property.PropertyType;
            this.PropertyName = property.Name;
            this.ColumnName = attr.Name;
            this.IsPk = attr.IsPrimaryKey;
            this.IsPartitionKey = attr.IsPartitionKey;
            this.IsAutoGenerated = attr.IsAutoGenerated;

            this.DbType = GetDbType(this.PropertyType);
        }

        public ColumnMapping(PropertyInfo property, ColumnAttribute attr, PropertyInfo parentPropertyInfo) : this(property, attr)
        {
            this.ParentPropertyInfo = parentPropertyInfo;
        }

        public ColumnMapping(ColumnMappingSettings columnMapping, Type type)
        {
            try
            {
                this.PropertyName = columnMapping.PropertyName;
                this.ColumnName = columnMapping.ColumnName;
                this.IsPk = columnMapping.IsPrimaryKey;
                this.IsPartitionKey = columnMapping.IsPartitionKey;

                if (String.IsNullOrEmpty(columnMapping.ParentPropertyName))
                    this.PropertyInfo = type.GetProperty(this.PropertyName);
                else
                {
                    this.ParentPropertyInfo = type.GetProperty(columnMapping.ParentPropertyName);
                    this.PropertyInfo = this.ParentPropertyInfo.PropertyType.GetProperty(this.PropertyName);
                }

                this.PropertyType = this.PropertyInfo.PropertyType;
                this.DbType = GetDbType(this.PropertyType);
            }
            catch (Exception ex) { Log.Error(String.Format("Error creating mapping for property [{0}] to column [{1}].", columnMapping.PropertyName, columnMapping.ColumnName), ex); }
        }

        /// <summary>
        /// Gets the value of the property represented by this mapping object in the passed in entity.
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public object GetValue(object entity)
        {
            return (this.ParentPropertyInfo == null) ?
                this.PropertyInfo.GetValue(entity, null) :
                this.PropertyInfo.GetValue(this.ParentPropertyInfo.GetValue(entity, null), null);
        }

        /// <summary>
        /// Sets the value of the property represented by this mapping object in the passed in entity.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="value"></param>
        public void SetValue(object entity, object value)
        {
            if (this.ParentPropertyInfo == null)
                PropertyInfo.SetValue(entity, value, null);
            else
            {
                object parentPropertyValue = ParentPropertyInfo.GetValue(entity, null);

                if(parentPropertyValue == null)
                    parentPropertyValue = Activator.CreateInstance(ParentPropertyInfo.PropertyType);

                // Set the value of the current nested property.
                PropertyInfo.SetValue(parentPropertyValue, value, null);

                // Set the value of the parent property.
                ParentPropertyInfo.SetValue(entity, parentPropertyValue, null);
            }
        }

        public static NpgsqlDbType GetDbType(Type type)
        {
            // If the property is an enum or nullable type then get the underlying type name.
            if (type.BaseType.Name.ToLower() == "enum")
                type = Enum.GetUnderlyingType(type);
            else if (type.Name.ToLower().StartsWith("nullable") && type.IsGenericType)
                type = type.GetGenericArguments()[0];

            switch (type.Name.ToLower())
            {
                case "string":
                    return NpgsqlDbType.Varchar;
                case "int16":
                    return NpgsqlDbType.Smallint;
                case "int32":
                    return NpgsqlDbType.Integer;
                case "int64":
                    return NpgsqlDbType.Bigint;
                case "decimal":
                    return NpgsqlDbType.Numeric;
                case "datetime":
                    return NpgsqlDbType.Timestamp;
                case "boolean":
                    return NpgsqlDbType.Boolean;
            }

            return NpgsqlDbType.Varchar;
        }
    }
}
