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
    public class EntityAdapter<ENTITY> : EntityAdapter, IEntityAdapter<ENTITY> where ENTITY : class, new()
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
        public virtual ENTITY Get(object id, bool forUpdate = false) { return DataMapper.Get<ENTITY>(SessionFactory.GetCurrentSession(SessionKey), id, forUpdate); }

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
        public virtual ENTITY Insert(ENTITY entity)
        {
            return (InsertInternal(entity) == 0) ? null : entity;
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
    }

    /// <summary>
    /// This class provides the interface that should be used to interact with the data persistence layer (currently the Postgres database).
    /// </summary>
    public abstract class EntityAdapter
    {
        /// <summary>
        /// Weakly typed method to get an entity by Id. If this entity has already been loaded
        /// it will be cached by NHibernate so it will not need to go to the database to load it again.
        /// </summary>
        /// <param name="entityName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public object Get(string entityName, object id) { return Get(Type.GetType(entityName), id); }

        /// <summary>
        /// Weakly typed method to get an entity by Id.
        /// </summary>
        /// <param name="entityName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public object Get(Type entityType, object id) { return Get(entityType, id, null); }

        /// <summary>
        /// Weakly typed method to get an entity by Id and partition key.
        /// </summary>
        /// <param name="entityName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public object Get(Type entityType, object id, object partitionKey)
        {
            return DataMapper.Get(entityType, SessionFactory.GetCurrentSession(entityType), id, partitionKey);
        }

        /// <summary>
        /// Gets a QueryBuilder instance for this entity type in order to query it.
        /// I made this static so it will work with static compiled queries. Not sure how else to make that work.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <returns></returns>
        public static QueryBuilder<ENTITY> Query<ENTITY>() where ENTITY : class, new()
        {
            return new QueryBuilder<ENTITY>();
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
        /// The entity tracker instance to use to track which entities have been inserted or deleted from the database.
        /// </summary>
        protected static IEntityTracker EntityTracker { get; set; }

        /*
     * I've decided not to allow this since it will allow anyone to easily modify entities without using the adapter specific to that entity.
     * For example, say I make a custom entity adapter for the User object called NoMattsEntityAdapter<User> which has custom code to check if a user's name is "Matt" and 
     * not let them in and I register that in the IoC to be what is used for any instances of IEntityAdapter<User>. Now if I allow this base IEntityAdapter interface/class
     * to be used then people could register a user named "Matt" and get around the check that was implemented.
     * 
        public virtual ENTITY Get<ENTITY>(object id, bool forUpdate = false) where ENTITY : class, new()
        {
            return DataMapper.Get<ENTITY>(SessionFactory.GetCurrentSession(typeof(ENTITY)), id, forUpdate);
        }

        public virtual QueryBuilder<ENTITY> Query<ENTITY>() where ENTITY : class, new()
        {
            return new QueryBuilder<ENTITY>();
        }

        public virtual ENTITY Insert<ENTITY>(ENTITY entity) where ENTITY : class, new()
        {
            return (InsertInternal(entity) == 0) ? null : entity;
        }

        public virtual void Update<ENTITY>(ENTITY entity) where ENTITY : class, new()
        {
            UpdateInternal(entity);
        }

        public virtual void Delete<ENTITY>(ENTITY entity) where ENTITY : class, new()
        {
            DeleteInternal(entity);
        }
         * */

        /// <summary>
        /// Sets the entity tracker instance to use with this entity adapter.
        /// </summary>
        /// <param name="entityTracker"></param>
        public static void SetEntityTracker(IEntityTracker entityTracker)
        {
            EntityTracker = entityTracker;
            Transaction.SetEntityTracker(entityTracker);
        }
    }
}
