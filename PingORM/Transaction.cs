using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace PingORM
{
    /// <summary>
    /// This class will handle nHibernate transactions and allow us to nest transactions.
    /// </summary>
    public class Transaction : IDisposable
    {
        static log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// The nHibernate transaction object that this uses.
        /// </summary>
        protected IDbTransaction DbTransaction { get; set; }

        /// <summary>
        /// The entity tracker instance to use to track which entities have been inserted or deleted from the database.
        /// </summary>
        protected static IEntityTracker EntityTracker { get; set; }

        /// <summary>
        /// Whether or not the underlying transaction represented by this instance is active.
        /// </summary>
        public bool IsActive { get { return DbTransaction != null; } }

        /// <summary>
        /// Default public constructor.
        /// </summary>
        public Transaction() { }

        /// <summary>
        /// Create a new instance of the Transaction class with an existing NHibernate transaction object.
        /// </summary>
        /// <param name="transaction"></param>
        public Transaction(IDbTransaction transaction) { DbTransaction = transaction; }

        /// <summary>
        /// Create a new instance of the Transaction class by creating a new transaction from the specified connection.
        /// </summary>
        /// <param name="connection"></param>
        public Transaction(IDbConnection connection) : this(connection.BeginTransaction()) { }

        /// <summary>
        /// Disposes of this Transaction instance and rolls back the active transaction if it is still active.
        /// </summary>
        public void Dispose()
        {
            if (DbTransaction != null)
            {
                try
                {
                    DbTransaction.Rollback();
                    Log.Error("Transaction: Transaction disposed without commit.");

                    if (EntityTracker != null)
                        EntityTracker.AfterCompletion(false);
                }
                finally { DbTransaction = null; }
            }
        }

        /// <summary>
        /// Commits this transaction.
        /// </summary>
        public void Commit()
        {
            if (DbTransaction != null)
            {
                try
                {
                    DbTransaction.Commit();

                    if (EntityTracker != null)
                        EntityTracker.AfterCompletion(true);
                }
                finally { DbTransaction = null; }
            }
        }

        /// <summary>
        /// Roll back this transaction.
        /// </summary>
        public void Rollback()
        {
            if (DbTransaction != null)
            {
                try
                {
                    DbTransaction.Rollback();

                    if (EntityTracker != null)
                        EntityTracker.AfterCompletion(false);
                }
                finally { DbTransaction = null; }
            }
        }

        /// <summary>
        /// Sets the entity tracker instance to use with this entity adapter.
        /// </summary>
        /// <param name="entityTracker"></param>
        internal static void SetEntityTracker(IEntityTracker entityTracker) { EntityTracker = entityTracker; }
    }

    /// <summary>
    /// A class to hold a collection of database transactions.
    /// </summary>
    public class TransactionCollection : IDisposable
    {
        static log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// The list of transactions contained in this collection.
        /// </summary>
        protected List<Transaction> Transactions { get; set; }

        /// <summary>
        /// Default public constructor.
        /// </summary>
        public TransactionCollection() { Transactions = new List<Transaction>(); }

        /// <summary>
        /// Add a new transaction to the collection.
        /// </summary>
        /// <param name="transaction"></param>
        public void Add(Transaction transaction) { Transactions.Add(transaction); }

        /// <summary>
        /// Disposes all of the transactions in the collection and rolls back any active transactions.
        /// </summary>
        public void Dispose()
        {
            foreach (Transaction transaction in Transactions)
            {
                if (transaction != null && transaction.IsActive)
                {
                    transaction.Rollback();
                    Log.Error("Transaction: Transaction disposed without commit.");
                }
            }
        }

        /// <summary>
        /// Commits all of the transactions in the collection.
        /// </summary>
        public void Commit()
        {
            foreach (Transaction transaction in Transactions)
            {
                if (transaction != null && transaction.IsActive)
                    transaction.Commit();
            }
        }

        /// <summary>
        /// Roll back all of the transactions in the collection.
        /// </summary>
        public void Rollback()
        {
            foreach (Transaction transaction in Transactions)
            {
                if (transaction != null && transaction.IsActive)
                    transaction.Rollback();
            }
        }
    }
}
