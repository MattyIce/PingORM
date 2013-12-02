using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Data;
using PingORM.Configuration;
using System.Data.SqlClient;
using Npgsql;
using MySql.Data.MySqlClient;

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

                // If caching is enabled for this entity type, check if it is in cache already.
                if (mapping.CachingEnabled)
                {
                    object cachedEntity = GetCachedEntity(type, id);

                    if (cachedEntity != null)
                        return cachedEntity;
                }

                // Make sure that the table is not partitioned.
                if (mapping.IsPartitioned && partitionKey == null)
                    throw new Exception("Cannot select an object from a partitioned table without a partition key.");

                //SqlCommand selectCommand = new SqlCommand(mapping.GetExpression, (SqlConnection)session.Connection);
                IDbCommand selectCommand = session.Connection.CreateCommand();
                selectCommand.CommandText = mapping.GetExpression;

                // Get the primary key column(s).
                List<ColumnMapping> idColumns = mapping.Columns.Where(c => c.IsPk).ToList();

                // Check if we have multiple PKs or just one.
                if (idColumns.Count == 1)
                    AddParameter(selectCommand, "p_id0", idColumns[0].DbType, id);
                else
                {
                    int i = 0;

                    // If we have multiple PKs then add a parameter for each of them.
                    foreach (ColumnMapping idColumn in idColumns)
                        AddParameter(selectCommand, String.Format("p_id{0}", i++), idColumn.DbType, idColumn.PropertyInfo.GetValue(id, null));
                }

                // Add the partition key timestamp parameter.
                if (mapping.IsPartitioned)
                {
                    ColumnMapping idColumn = mapping.Columns.FirstOrDefault(c => c.IsPartitionKey);
                    AddParameter(selectCommand, "p_ts", idColumn.DbType, partitionKey);
                }

                LogCommand(selectCommand);

                using (IDataReader reader = selectCommand.ExecuteReader())
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
        /// Gets a string identifier of the specified entity based upon the Primary Key defined in the mapping.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        public static string GetIdString(object entity)
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(entity.GetType());

            TableMapping mapping = _mappings[entity.GetType()];

            // Get the primary key column(s).
            List<ColumnMapping> idColumns = mapping.Columns.Where(c => c.IsPk).ToList();

            // Check if we have multiple PKs or just one.
            if (idColumns.Count == 1)
                return String.Format("{0}:{1}", entity.GetType().Name, idColumns[0].PropertyInfo.GetValue(entity));
            else
            {
                StringBuilder retVal = new StringBuilder(entity.GetType().Name);

                foreach (ColumnMapping idColumn in idColumns)
                    retVal.Append(":").Append(idColumn.PropertyInfo.GetValue(entity));

                return retVal.ToString();
            }
        }

        /// <summary>
        /// Reads an object of the specified type from the specified data reader.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="reader"></param>
        /// <returns></returns>
        private static object FromDb(Type type, IDataReader reader)
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

            if(mapping.CachingEnabled)
                CacheEntity(record);

            return record;
        }

        /// <summary>
        /// Executes the select statement specified by the provided query builder.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="session"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static IEnumerable<T> Select<T>(ISession session, QueryBuilder<T> query) where T : class, new()
        {
            // Make sure the mappings have been loaded for this type.
            LoadMappings(typeof(T));

            IDbCommand selectCommand = session.Connection.CreateCommand();
            selectCommand.CommandText = query.ToString();

            if (query.Parameters != null)
            {
                // Add the parameters from the query builder.
                foreach (var parameter in query.Parameters)
                    AddParameter(selectCommand, parameter.Key, ColumnMapping.GetDbType(parameter.Value.Value.GetType()), parameter.Value.Value);
            }

            List<T> results = new List<T>();

            LogCommand(selectCommand);
            using (IDataReader reader = selectCommand.ExecuteReader())
            {
                while (reader.Read())
                    results.Add(FromDb(typeof(T), reader) as T);
            }

            return results;
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
                IDbCommand selectCommand = session.Connection.CreateCommand();
                selectCommand.CommandText = query.ToString();

                if (query.Parameters != null)
                {
                    // Add the parameters from the query builder.
                    foreach (var parameter in query.Parameters)
                        AddParameter(selectCommand, parameter.Key, ColumnMapping.GetDbType(parameter.Value.Value.GetType()), parameter.Value.Value);
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
                IDbCommand insertCommand = session.Connection.CreateCommand();
                insertCommand.CommandText = mapping.InsertExpression;

                int i = 0;
                foreach (ColumnMapping column in mapping.Columns)
                {
                    if (SessionFactory.Provider == DataProvider.MySql && column.IsAutoGenerated)
                        continue;

                    // For Postgres we need to get the next sequence value for the ID.
                    if (column.IsPk && !String.IsNullOrEmpty(mapping.SequenceName) && SessionFactory.Provider == DataProvider.Postgres)
                    {
                        IDbCommand seqCommand = session.Connection.CreateCommand();
                        seqCommand.CommandText = String.Format("select nextval('\"{0}\"')", mapping.SequenceName);
                        LogCommand(seqCommand);
                        column.PropertyInfo.SetValue(entity, Convert.ChangeType(seqCommand.ExecuteScalar(), column.PropertyType), null);
                    }

                    AddParameter(insertCommand, String.Format("p{0}", i++), column.DbType, column.GetValue(entity));
                }

                LogCommand(insertCommand);
                int result = insertCommand.ExecuteNonQuery();

                // For MySQL we need to get the ID of the last generated auto_increment column.
                if (insertCommand is MySqlCommand)
                {
                    ColumnMapping column = mapping.Columns.FirstOrDefault(c => c.IsAutoGenerated);

                    if(column != null)
                        column.PropertyInfo.SetValue(entity, ((MySqlCommand)insertCommand).LastInsertedId);
                }

                CacheEntity(entity);

                return result;
            }
            catch (Exception ex) { Log.Error("DataMapper.Insert threw an exception.", ex); }

            return 0;
        }

        internal static void CacheEntity(object entity)
        {
            SessionFactory.SessionStorage.SetCurrent(GetIdString(entity), entity);
        }

        internal static object GetCachedEntity(Type type, object id)
        {
            return SessionFactory.SessionStorage.GetCurrent<object>(String.Format("{0}:{1}", type.Name, id));
        }

        internal static void ClearCachedEntity(object entity)
        {
            SessionFactory.SessionStorage.SetCurrent(GetIdString(entity), null);
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
                IDbCommand command = session.Connection.CreateCommand();
                command.CommandText = query.ToString();

                if (query.Parameters != null)
                {
                    // Add the parameters from the query builder.
                    foreach (var parameter in query.Parameters)
                        AddParameter(command, parameter.Key, ColumnMapping.GetDbType(parameter.Value.Value.GetType()), parameter.Value.Value);
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
                IDbCommand updateCommand = session.Connection.CreateCommand();
                updateCommand.CommandText = mapping.UpdateExpression;

                int i = 0;
                foreach (ColumnMapping column in mapping.Columns)
                    AddParameter(updateCommand, String.Format("p{0}", i++), column.DbType, column.GetValue(entity));

                LogCommand(updateCommand);
                int rows = updateCommand.ExecuteNonQuery();

                if (rows > 0 && mapping.CachingEnabled)
                    CacheEntity(entity);

                return rows;
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
                IDbCommand deleteCommand = session.Connection.CreateCommand();
                deleteCommand.CommandText = mapping.DeleteExpression;

                int i = 0;
                foreach (ColumnMapping column in mapping.Columns.Where(c => c.IsPk))
                    AddParameter(deleteCommand, String.Format("p{0}", i++), column.DbType, column.GetValue(entity));

                LogCommand(deleteCommand);
                int rows = deleteCommand.ExecuteNonQuery();

                if (rows > 0 && mapping.CachingEnabled)
                    ClearCachedEntity(entity);

                return rows;
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
                IDbCommand command = session.Connection.CreateCommand();
                command.CommandText = sql;

                if (parameters != null)
                {
                    foreach (KeyValuePair<string, object> kvp in parameters)
                        AddParameter(command, kvp.Key, ColumnMapping.GetDbType(kvp.Value.GetType()), kvp.Value);
                }

                LogCommand(command);
                using (IDataReader reader = command.ExecuteReader())
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

        private static void AddParameter(IDbCommand command, string paramName, DbType type, object value)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = paramName;
            param.DbType = type;
            param.Value = value;
            command.Parameters.Add(param);
        }

        /// <summary>
        /// Logs a sql command to the SQL log4net log.
        /// </summary>
        /// <param name="command"></param>
        static void LogCommand(IDbCommand command)
        {
            if (SqlLog.IsDebugEnabled)
            {
                StringBuilder sb = new StringBuilder(command.CommandText);

                foreach (IDbDataParameter param in command.Parameters)
                    sb.Append(String.Format(" :{0} = {1} [Type: {2}],", param.ParameterName, param.Value, param.DbType));

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

        internal static string EscapeName(string name)
        {
            return (SessionFactory.Provider == DataProvider.Postgres) ? String.Format("\"{0}\"", name) : name;
        }

        internal static string ParamName(string name)
        {
            return ParamName(name, false);
        }

        internal static string ParamName(string name, bool isConst)
        {
            return (SessionFactory.Provider == DataProvider.Postgres) ?
                String.Format(":{0}{1}", isConst ? "c" : "p", name) :
                String.Format("@{0}{1}", isConst ? "c" : "p", name);
        }

        internal static string ParamName(int name)
        {
            return ParamName(name, false);
        }

        internal static string ParamName(int name, bool isConst)
        {
            return ParamName(name.ToString(), false);
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
        public bool CachingEnabled { get; set; }

        public TableMapping()
        {
            this.Columns = new List<ColumnMapping>();
        }

        public TableMapping(DataEntityAttribute tableAttribute, Type type) : this()
        {
            this.Key = tableAttribute.Key;
            this.TableName = tableAttribute.TableName;
            this.SequenceName = tableAttribute.SequenceName;
            this.CachingEnabled = !tableAttribute.DisableCaching;

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
                if (SessionFactory.Provider == DataProvider.MySql && column.IsAutoGenerated)
                    continue;

                columnStr.Append(String.Format("{0},", DataMapper.EscapeName(column.ColumnName)));
                valueStr.Append(String.Format("{0},", DataMapper.ParamName(i++)));
            }

            this.InsertExpression = String.Format("INSERT INTO {0} ({1}) VALUES ({2});", DataMapper.EscapeName(this.TableName),
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

                whereStr.Append(String.Format("{0} = {1}", DataMapper.EscapeName(column.ColumnName), DataMapper.ParamName(i++)));
            }

            this.DeleteExpression = String.Format("DELETE FROM {0} WHERE {1};", DataMapper.EscapeName(this.TableName), whereStr.ToString());
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

                    whereStr.Append(String.Format("{0} = {1}", DataMapper.EscapeName(column.ColumnName), DataMapper.ParamName(i++)));
                }
                else
                    columnStr.Append(String.Format("{0} = {1},", DataMapper.EscapeName(column.ColumnName), DataMapper.ParamName(i++)));
            }

            this.UpdateExpression = String.Format("UPDATE {0} SET {1} WHERE {2};", DataMapper.EscapeName(this.TableName),
                columnStr.ToString().TrimEnd(new char[] { ',' }), whereStr.ToString());
        }

        public void GenerateSelectExpression()
        {
            StringBuilder columnStr = new StringBuilder();
            StringBuilder whereStr = new StringBuilder();

            foreach (ColumnMapping column in Columns)
                columnStr.Append(String.Format("{0},", DataMapper.EscapeName(column.ColumnName)));

            this.SelectExpression = String.Format("SELECT {0} FROM {1}", columnStr.ToString().TrimEnd(new char[] { ',' }), DataMapper.EscapeName(this.TableName));
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

                    whereStr.Append(String.Format("{0} = {1}", DataMapper.EscapeName(column.ColumnName), DataMapper.ParamName(column.IsPk ? String.Format("_id{0}", i++) : "ts")));
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
        public DbType DbType { get; set; }
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

        public static DbType GetDbType(Type type)
        {
            // If the property is an enum or nullable type then get the underlying type name.
            if (type.BaseType.Name.ToLower() == "enum")
                type = Enum.GetUnderlyingType(type);
            else if (type.Name.ToLower().StartsWith("nullable") && type.IsGenericType)
                type = type.GetGenericArguments()[0];

            switch (type.Name.ToLower())
            {
                case "string":
                    return DbType.String;
                case "int16":
                    return DbType.Int16;
                case "int32":
                    return DbType.Int32;
                case "int64":
                    return DbType.Int64;
                case "decimal":
                    return DbType.Decimal;
                case "double":
                    return DbType.Double;
                case "datetime":
                    return DbType.DateTime;
                case "boolean":
                    return DbType.Boolean;
                case "guid":
                    return DbType.Guid;
            }

            return DbType.String;
        }
    }
}
