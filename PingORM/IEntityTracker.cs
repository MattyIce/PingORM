using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PingORM
{
    /// <summary>
    /// Implement this interface to track what new entities have been inserted into the database.
    /// </summary>
    public interface IEntityTracker
    {
        /// <summary>
        /// Record that a new entity was inserted into the database.
        /// </summary>
        /// <param name="entity"></param>
        void EntityInserted(object entity);

        /// <summary>
        /// Record that an existing entity was deleted from the database.
        /// </summary>
        /// <param name="entity"></param>
        void EntityDeleted(object entity);

        /// <summary>
        /// Called after the transaction has been completed.
        /// </summary>
        /// <param name="success">Whether or not the transaction was successful.</param>
        void AfterCompletion(bool success);
    }
}
