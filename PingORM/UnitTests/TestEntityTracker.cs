using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PingORM.UnitTests
{
    /// <summary>
    /// Entity adapter used for unit tests, it will keep track of all entities added so that they can be removed after the test completes.
    /// </summary>
    public class TestEntityTracker : IEntityTracker
    {
        /// <summary>
        /// The hierarchy of the entities in the database so we know which to delete first when we're doing cleanup.
        /// There is probably a better way to do this than a hard-coded string list, but it works for now.
        /// </summary>
        private static List<string> _entityHierarchy = new List<string>();

        /// <summary>
        /// The list of entities that have been inserted by the unit tests.
        /// </summary>
        private List<object> _insertedEntities = new List<object>();

        /// <summary>
        /// The list of entities that will be inserted when the transaction completes.
        /// </summary>
        private List<object> _transientInsertedEntities = new List<object>();

        /// <summary>
        /// The list of entities that will be deleted when the transaction completes.
        /// </summary>
        private List<object> _transientDeletedEntities = new List<object>();

        /// <summary>
        /// Sets the hierarchy of the entities so that test entities can be deleted in the correct order so as not to violate any foreign key constraints.
        /// The list should contain the name of all of the data entities that will be used in the tests. The entities will be deleted in the order that
        /// they appear in the list, so child entities should appear before their parent entities in the list.
        /// </summary>
        /// <param name="entityHierarchy"></param>
        public static void SetEntityHierarchy(List<string> entityHierarchy) { _entityHierarchy = entityHierarchy; }

        /// <summary>
        /// This method inserts a new entity into the database.
        /// </summary>
        /// <param name="entity"></param>
        public void EntityInserted(object entity)
        {
            _transientInsertedEntities.Add(entity);
        }

        /// <summary>
        /// This method deletes an existing entity from the database.
        /// </summary>
        /// <param name="entity"></param>
        public void EntityDeleted(object entity)
        {
            _transientDeletedEntities.Add(entity);
        }

        /// <summary>
        /// Called after the transaction completes, either committed or rolled back.
        /// </summary>
        /// <param name="success"></param>
        public void AfterCompletion(bool success)
        {
            if (success)
            {
                _insertedEntities.AddRange(_transientInsertedEntities);
                _insertedEntities.RemoveAll(e => _transientDeletedEntities.Contains(e));
            }

            _transientInsertedEntities.Clear();
            _transientDeletedEntities.Clear();
        }

        /// <summary>
        /// Called before the transaction is completed.
        /// </summary>
        public void BeforeCompletion() { }

        /// <summary>
        /// This method removes all entities that have been inserted by the unit tests.
        /// </summary>
        public void CleanUp()
        {
            TestEntityAdapter adapter = new TestEntityAdapter();

            // Delete entities in the appropriate order based on the hierarchy.
            foreach (object entity in _insertedEntities.OrderBy(e => _entityHierarchy.IndexOf(e.GetType().Name)))
            {
                using (Transaction transaction = SessionFactory.GetTransaction(entity.GetType()))
                {
                    adapter.Delete(entity);
                    transaction.Commit();
                }
            }
        }
    }
}
