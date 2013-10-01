using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using PingORM.Configuration;

namespace PingORM
{
    /// <summary>
    /// Generically typed class that provides the interface that should be used to interact with the data persistence layer (currently the Postgres database).
    /// </summary>
    /// <typeparam name="ENTITY"></typeparam>
    public class EntityAdapter<ENTITY> : EntityAdapter,  IEntityUpdater<ENTITY> where ENTITY : class, new()
    {
        /// <summary>
        /// The database session key for the ENTITY type.
        /// </summary>
        protected string SessionKey { get; set; }

        /// <summary>
        /// Default public constructor.
        /// </summary>
        public EntityAdapter() { SessionKey = SessionFactory.GetSessionKey(typeof(ENTITY)); }

        /// <summary>
        /// Stronly typed method to get an entity by Id. This will load the entity from the database.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="id"></param>
        /// <returns></returns>
        public virtual ENTITY Get(object id) { return DataMapper.Get<ENTITY>(SessionFactory.GetCurrentSession(SessionKey), id); }

        /// <summary>
        /// Gets an entity from a partitioned table in the db by its ID and partition key timestamp.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        public virtual ENTITY Get(object id, object partitionKey)
        {
            return DataMapper.Get<ENTITY>(SessionFactory.GetCurrentSession(SessionKey), id, partitionKey);
        }

        /// <summary>
        /// Gets a QueryBuilder instance for this entity in order to query it.
        /// </summary>
        /// <returns></returns>
        public virtual QueryBuilder<ENTITY> Query() { return new QueryBuilder<ENTITY>(); }

        /// <summary>
        /// This method inserts a new entity into the database.
        /// </summary>
        /// <param name="entity"></param>
        public virtual int Insert(ENTITY entity)
        {
            return InsertInternal(entity);
        }

        /// <summary>
        /// This method updates the values of an existing entity in the database.
        /// </summary>
        /// <param name="entity"></param>
        public virtual void Update(ENTITY entity)
        {
            UpdateInternal(entity);
        }

        /// <summary>
        /// This method deletes an existing entity in the database.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="entity"></param>
        public virtual void Delete(ENTITY entity)
        {
            DeleteInternal(entity);
        }

        /// <summary>
        /// Gets the entity adapter that has been registered for the specified type.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <returns></returns>
        public static EntityAdapter<ENTITY> GetAdapter() { return EntityAdapter.GetAdapter<ENTITY>(); }
    }

    /// <summary>
    /// This class provides the interface that should be used to interact with the data persistence layer (currently the Postgres database).
    /// </summary>
    public abstract class EntityAdapter
    {
        /// <summary>
        /// Stronly typed method to get an entity by Id. If this entity has already been loaded
        /// it will be cached by NHibernate so it will not need to go to the database to load it again.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="id"></param>
        /// <returns></returns>
        public static ENTITY Get<ENTITY>(object id) where ENTITY : class, new()
        {
            return GetAdapter<ENTITY>().Get(id);
        }

        /// <summary>
        /// Gets an entity from a partitioned table in the db by its ID and partition key timestamp.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="id"></param>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public static ENTITY Get<ENTITY>(object id, object partitionKey) where ENTITY : class, new()
        {
            return GetAdapter<ENTITY>().Get(id, partitionKey);
        }

        /// <summary>
        /// Weakly typed method to get an entity by Id. If this entity has already been loaded
        /// it will be cached by NHibernate so it will not need to go to the database to load it again.
        /// </summary>
        /// <param name="entityName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public static object Get(string entityName, object id) { return Get(Type.GetType(entityName), id); }

        /// <summary>
        /// Weakly typed method to get an entity by Id.
        /// </summary>
        /// <param name="entityName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public static object Get(Type entityType, object id) { return Get(entityType, id, null); }

        /// <summary>
        /// Weakly typed method to get an entity by Id and partition key.
        /// </summary>
        /// <param name="entityName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public static object Get(Type entityType, object id, object partitionKey)
        {
            return DataMapper.Get(entityType, SessionFactory.GetCurrentSession(entityType), id, partitionKey);
        }

        /// <summary>
        /// Gets a QueryBuilder instance for this entity in order to query it.
        /// </summary>
        /// <returns></returns>
        public static QueryBuilder<ENTITY> Query<ENTITY>() where ENTITY : class, new()
        {
            return GetAdapter<ENTITY>().Query();
        }

        /// <summary>
        /// This method inserts a new entity into the database.
        /// </summary>
        /// <param name="entity"></param>
        protected virtual int InsertInternal(object entity)
        {
            ISession session = SessionFactory.GetCurrentSession(entity.GetType());

            // Track the insertion of this entity.
            if (EntityTracker != null)
                EntityTracker.EntityInserted(entity);

            int retVal = 0;

            using (Transaction transaction = SessionFactory.GetTransaction(entity.GetType()))
            {
                retVal = DataMapper.Insert(session, entity);
                transaction.Commit();
            }

            return retVal;
        }

        /// <summary>
        /// This method updates the values of an existing entity in the database.
        /// </summary>
        /// <param name="entity"></param>
        protected virtual void UpdateInternal(object entity)
        {
            ISession session = SessionFactory.GetCurrentSession(entity.GetType());

            using (Transaction transaction = SessionFactory.GetTransaction(entity.GetType()))
            {
                DataMapper.Update(session, entity);
                transaction.Commit();
            }
        }

        /// <summary>
        /// This method deletes an existing entity in the database.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="entity"></param>
        protected virtual void DeleteInternal(object entity)
        {
            ISession session = SessionFactory.GetCurrentSession(entity.GetType());

            // Track the deletion of this entity.
            if (EntityTracker != null)
                EntityTracker.EntityDeleted(entity);

            // Since this is a newly created entity object, we need to merge it with the one currently in the 
            // NHibernate cache (if there is one) or it will throw an exception.
            //entity = session.Merge(entity);

            using (Transaction transaction = SessionFactory.GetTransaction(entity.GetType()))
            {
                DataMapper.Delete(session, entity);
                transaction.Commit();
            }
        }

        /// <summary>
        /// Inserts a new entity into the database.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="entity"></param>
        public static ENTITY Insert<ENTITY>(ENTITY entity) where ENTITY : class, new()
        {
            return (GetUpdater<ENTITY>().Insert(entity) == 0) ? null : entity;
        }

        /// <summary>
        /// Inserts a new entity into the database.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="entity"></param>
        public static void Update<ENTITY>(ENTITY entity) where ENTITY : class, new()
        {
            GetUpdater<ENTITY>().Update(entity);
        }

        /// <summary>
        /// Inserts a new entity into the database.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="entity"></param>
        public static void Delete<ENTITY>(ENTITY entity) where ENTITY : class, new()
        {
            GetUpdater<ENTITY>().Delete(entity);
        }

        /// <summary>
        /// Register adapter classes in the business objects assembly.
        /// </summary>
        /// <param name="assembly">The assembly containing the business object adapters.</param>
        public static void RegisterAdapters(Assembly assembly)
        {
            // Get all of the types that implement IEntityAdapter.
            foreach (Type type in assembly.GetTypes().Where(t => t.IsSubclassOf(typeof(EntityAdapter))))
            {
                // Register types that have the EntityAdapter attribute.
                foreach (EntityAdapterAttribute attribute in type.GetCustomAttributes(typeof(EntityAdapterAttribute), false))
                {
                    EntityAdapter adapter = Activator.CreateInstance(type) as EntityAdapter;
                    RegisterAdapter(attribute.EntityType, adapter);
                }
            }
        }

        /// <summary>
        /// The entity tracker instance to use to track which entities have been inserted or deleted from the database.
        /// </summary>
        protected static IEntityTracker EntityTracker { get; set; }

        /// <summary>
        /// Sets the entity tracker instance to use with this entity adapter.
        /// </summary>
        /// <param name="entityTracker"></param>
        public static void SetEntityTracker(IEntityTracker entityTracker)
        {
            EntityTracker = entityTracker;
            Transaction.SetEntityTracker(entityTracker);
        }

        /// <summary>
        /// List of entity adapter instances by entity type.
        /// </summary>
        private static Dictionary<Type, EntityAdapter> _adapters = new Dictionary<Type, EntityAdapter>();

        /// <summary>
        /// Registers an adapter for an entity type.
        /// </summary>
        /// <param name="entityType"></param>
        /// <param name="adapter"></param>
        public static void RegisterAdapter(Type entityType, EntityAdapter adapter)
        {
            // Make sure no adapter is already registered for this entity type.
            if (_adapters.ContainsKey(entityType))
                throw new Exception(String.Format("An adapter has already been registered for entity type [{0}].", entityType.FullName));

            _adapters.Add(entityType, adapter);
        }

        /// <summary>
        /// Gets the entity adapter that has been registered for the specified type.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <returns></returns>
        protected static EntityAdapter<ENTITY> GetAdapter<ENTITY>() where ENTITY : class, new()
        {
            // Return the adapter registered for the specified type, or the default adapter if none is registered.
            return _adapters.ContainsKey(typeof(ENTITY)) ? _adapters[typeof(ENTITY)] as EntityAdapter<ENTITY> : new EntityAdapter<ENTITY>();
        }

        /// <summary>
        /// Gets the entity adapter that has been registered for the specified type.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <returns></returns>
        protected static IEntityUpdater<ENTITY> GetUpdater<ENTITY>() where ENTITY : class, new()
        {
            // Return the adapter registered for the specified type, or the default adapter if none is registered.
            return _adapters.ContainsKey(typeof(ENTITY)) ? _adapters[typeof(ENTITY)] as IEntityUpdater<ENTITY> : new EntityAdapter<ENTITY>();
        }
    }
}
