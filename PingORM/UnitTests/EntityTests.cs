using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using NUnit.Framework;

namespace PingORM.UnitTests
{
    /// <summary>
    /// Class for testing CRUD operations on data entities.
    /// </summary>
    public class EntityTests
    {
        /// <summary>
        /// Generic method that tests adding an entity given its type name.
        /// </summary>
        /// <param name="typeName"></param>
        public static void AddEntity(string typeName, string excludeProperties, string @namespace)
        {
            Type type = Type.GetType(String.Format("{0}.{1}, {0}", @namespace, typeName));
            Assert.IsNotNull(type, String.Format("Type [{0}.{1}, {0}] not found.", @namespace, typeName));

            // Add a new entity of this type to the database.
            object entity = AddEntity(type);

            // Start a new db session so the entity that was just inserted will not be cached.
            SessionFactory.StartNewSession(type);

            // Read the entity that was just inserted from the db.
            var fromDb = new TestEntityAdapter().Get(type, DataMapper.GetId(entity));
            Assert.IsNotNull(fromDb);

            // Test that all of the property values are equal in the original entity and the one returned from the db.
            TestEqualProperties(entity, fromDb, excludeProperties);
        }

        /// <summary>
        /// Generic method that tests updating an existing entity in the database given it's type name.
        /// </summary>
        /// <param name="typeName"></param>
        /// <param name="excludeProperties"></param>
        public static void UpdateEntity(string typeName, string excludeProperties, string @namespace)
        {
            Type type = Type.GetType(String.Format("{0}.{1}, {0}", @namespace, typeName));
            Assert.IsNotNull(type, String.Format("Type [{0}.{1}, {0}] not found.", @namespace, typeName));

            // Add a new entity of this type to the database.
            object entity = AddEntity(type);

            // Randomly set all the properties in the entity to new random values.
            SetProperties(entity, true, true);

            // Update the entity in the database.
            new TestEntityAdapter().Update(entity);

            // Start a new db session so the entity that was just updated will not be cached.
            SessionFactory.StartNewSession(type);

            // Read the entity that was just inserted from the db.
            var fromDb = new TestEntityAdapter().Get(type, DataMapper.GetId(entity));
            Assert.IsNotNull(fromDb);

            // Test that all of the property values are equal in the original entity and the one returned from the db.
            TestEqualProperties(entity, fromDb, excludeProperties);
        }

        /// <summary>
        /// Generic method that tests deleting an existing entity from the database.
        /// </summary>
        /// <param name="typeName"></param>
        public static void DeleteEntity(string typeName, string @namespace)
        {
            Type type = Type.GetType(String.Format("{0}.{1}, {0}", @namespace, typeName));
            Assert.IsNotNull(type, String.Format("Type [{0}.{1}, {0}] not found.", @namespace, typeName));

            // Add a new entity of this type to the database.
            object entity = AddEntity(type);

            // Delete the newly inserted entity from the database.
            new TestEntityAdapter().Delete(entity);

            // Start a new db session so the entity that was just deleted will not be cached.
            SessionFactory.StartNewSession(type);

            // Read the entity that was just inserted from the db.
            var fromDb = new TestEntityAdapter().Get(type, DataMapper.GetId(entity));
            Assert.IsNull(fromDb);
        }

        /// <summary>
        /// Tests whether the non-class properties of two entities are equal.
        /// </summary>
        /// <param name="entity1"></param>
        /// <param name="entity2"></param>
        /// <param name="excludeProperties">A comma-separated list of properties to exclude from the comparisons.</param>
        public static void TestEqualProperties(object entity1, object entity2, string excludeProperties)
        {
            // Make sure the entities are of the same type.
            Assert.AreEqual(entity1.GetType().FullName, entity2.GetType().FullName);

            TableMapping mapping = DataMapper.GetTableMapping(entity1.GetType());

            // Compare all of the columns in the mapping.
            foreach (ColumnMapping columnMapping in mapping.Columns)
                Assert.AreEqual(columnMapping.GetValue(entity1), columnMapping.GetValue(entity2), String.Format("Values of property [{0}] do not match.", columnMapping.PropertyName));
        }

        /// <summary>
        /// Sets all properties on the passed in entity to random values.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="createParentEntities">Whether or not to create new parent entities and set the reference values.</param>
        /// <param name="isUpdate">Whether or not the properties are being updated (as opposed to initialized for insert).</param>
        protected static void SetProperties(object entity, bool createParentEntities, bool isUpdate)
        {
            // Create a new random number generator using the current milliseconds as the seed value.
            Random r = new Random(DateTime.Now.Millisecond);

            TableMapping mapping = DataMapper.GetTableMapping(entity.GetType());

            // Set each of the entity's properties.
            foreach (ColumnMapping columnMapping in mapping.Columns)
            {
                // Don't set the value if it's an auto-generated column or the primary or partition key for updates.
                if (columnMapping.IsAutoGenerated || (isUpdate && (columnMapping.IsPartitionKey || columnMapping.IsPk)) || (columnMapping.IsPk && !String.IsNullOrEmpty(mapping.SequenceName)))
                    continue;

                object value = null;

                if (columnMapping.ParentPropertyInfo != null)
                {
                    // This is a nested type, see if it has already been created or create a new one.
                    value = columnMapping.ParentPropertyInfo.GetValue(entity, null);

                    if (value == null)
                        value = Activator.CreateInstance(columnMapping.ParentPropertyInfo.PropertyType);

                    // Set the value of the current nested property.
                    columnMapping.PropertyInfo.SetValue(value, GetRandomValue(columnMapping.PropertyType), null);
                }
                else
                    value = GetRandomValue(columnMapping.PropertyType);

                if (value != null)
                {
                    if (columnMapping.ParentPropertyInfo != null)
                        columnMapping.ParentPropertyInfo.SetValue(entity, value, null);
                    else
                        columnMapping.PropertyInfo.SetValue(entity, value, null);
                }
            }
        }

        /// <summary>
        /// Generates a random value of the specified type.
        /// </summary>
        /// <param name="propertyType"></param>
        /// <returns></returns>
        protected static object GetRandomValue(Type propertyType)
        {
            // Create a new random number generator using the current milliseconds as the seed value.
            Random r = new Random(DateTime.Now.Millisecond);

            if (propertyType.IsEnum)
            {
                // Get a random enum value.
                Array enumValues = Enum.GetValues(propertyType);
                return enumValues.GetValue(r.Next(enumValues.Length));
            }
            else
            {
                switch (propertyType.Name.ToLower())
                {
                    case "int16":
                        return Convert.ToInt16(r.Next(10000));
                    case "int32":
                        return Convert.ToInt32(r.Next(10000));
                    case "int64":
                        return Convert.ToInt64(r.Next(10000));
                    case "decimal":
                        return Convert.ToDecimal(r.Next(10000));
                    case "double":
                        return Convert.ToDouble(r.Next(10000));
                    case "string":
                        return String.Format("Test_{0}", r.Next(10000));
                    case "datetime":
                        return DateTime.Now;
                }
            }

            return null;
        }

        /// <summary>
        /// Generic method that tests adding an entity given its type name.
        /// </summary>
        /// <param name="typeName"></param>
        protected static object AddEntity(Type type)
        {
            // Create an instance of the passed in type.
            object entity = Activator.CreateInstance(type);

            // Set all of the properties on the entity to random values.
            SetProperties(entity, true, false);

            // Add the new entity to the database.
            new TestEntityAdapter().Insert(entity);

            return entity;
        }
    }
}
