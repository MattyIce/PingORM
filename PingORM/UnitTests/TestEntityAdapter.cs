using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PingORM.UnitTests
{
    /// <summary>
    /// This is needed to be able to crud entities without knowing their type at compile time.
    /// </summary>
    public class TestEntityAdapter : EntityAdapter
    {
        /// <summary>
        /// This method inserts a new entity into the database.
        /// </summary>
        /// <param name="entity"></param>
        public virtual void Insert(object entity) { InsertInternal(entity); }

        /// <summary>
        /// This method updates the values of an existing entity in the database.
        /// </summary>
        /// <param name="entity"></param>
        public virtual void Update(object entity) { UpdateInternal(entity); }

        /// <summary>
        /// This method deletes an existing entity in the database.
        /// </summary>
        /// <typeparam name="ENTITY"></typeparam>
        /// <param name="entity"></param>
        public virtual void Delete(object entity) { DeleteInternal(entity); }
    }
}
