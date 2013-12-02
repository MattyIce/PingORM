using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Data;
using PingORM.Configuration;
using PingORM.Utilities;
using System.Configuration;

namespace PingORM
{
    public enum DataProvider
    {
        Postgres,
        MySql
    }

    public class SessionFactory
    {
        static log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        internal static IKeyStorage SessionStorage;
        internal static DataProvider Provider = DataProvider.Postgres;

        /// <summary>
        /// Initialize the session factory from the connectionSettings configuration element.
        /// </summary>
        public static void Initialize() { Initialize(new StaticKeyStorage()); }

        /// <summary>
        /// Initialize the session factory from the connectionSettings configuration element using web session storage.
        /// </summary>
        public static void InitializeWeb() { Initialize(new WebKeyStorage()); }

        /// <summary>
        /// Initialize the session factory from the connectionSettings configuration element using web session storage for a specific data provider.
        /// </summary>
        public static void InitializeWeb(DataProvider provider)
        {
            Provider = provider;
            InitializeWeb();
        }

        /// <summary>
        /// Initialize the session factory from the connectionSettings configuration element.
        /// </summary>
        /// <param name="sessionStorage"></param>
        public static void Initialize(IKeyStorage sessionStorage)
        {
            SetSessionStorage(sessionStorage);
        }

        /// <summary>
        /// Sets the current sessage storage object.
        /// </summary>
        /// <param name="sessionStorage"></param>
        public static void SetSessionStorage(IKeyStorage sessionStorage) { SessionStorage = sessionStorage; }

        /// <summary>
        /// Disconnects the current session and starts a new one.
        /// </summary>
        /// <param name="key"></param>
        public static void StartNewSession(string key)
        {
            if (SessionStorage == null)
                return;

            ISession session = SessionStorage.GetCurrent<ISession>(key);

            if (session != null && session.IsConnected)
                session.Close();
        }

        /// <summary>
        /// Disconnects the current session and starts a new one.
        /// </summary>
        /// <param name="entityType"></param>
        public static void StartNewSession(Type entityType)
        {
            TableMapping mapping = DataMapper.GetTableMapping(entityType);

            // Make sure this type has been registered with a session key.
            if (mapping != null)
                StartNewSession(mapping.Key);
        }

        /// <summary>
        /// Gets the current session saved in the session storage object.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        internal static ISession GetCurrentSession(string key)
        {
            ConnectionStringSettings connectionElement = ConfigurationManager.ConnectionStrings[key];

            if (connectionElement == null)
                throw new Exception("No connection string found for the specified key.");

            if (SessionStorage != null)
            {
                ISession session = SessionStorage.GetCurrent<ISession>(key);

                // Make sure an open session is stored.
                if (session == null || !session.IsConnected)
                    SessionStorage.SetCurrent(key, session = new DataSession(connectionElement.ConnectionString));

                return session;
            }
            else
                return new DataSession(connectionElement.ConnectionString);
        }

        /// <summary>
        /// Gets the current session for the specified entity type.
        /// </summary>
        /// <param name="entityType"></param>
        /// <returns></returns>
        internal static ISession GetCurrentSession(Type entityType)
        {
            TableMapping mapping = DataMapper.GetTableMapping(entityType);

            // Make sure this type has been registered with a session key.
            if (mapping == null)
                return null;

            return GetCurrentSession(mapping.Key);
        }

        /// <summary>
        /// Gets a new session separate from the current session used by most queries.
        /// This should be used when forking off a new thread to refresh cache asynchronously, for example.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        internal static ISession GetNewSession(string key)
        {
            ConnectionStringSettings connectionElement = ConfigurationManager.ConnectionStrings[key];

            if(connectionElement == null)
                throw new Exception("No connection string found for the specified key.");

            return new DataSession(connectionElement.ConnectionString);
        }

        /// <summary>
        /// Ends the current DB session.
        /// </summary>
        /// <param name="key"></param>
        public static void EndCurrentSession(string key) 
        {
            ISession session = SessionStorage.GetCurrent<ISession>(key);

            if (session != null && session.IsConnected)
                session.Close();
        }

        /// <summary>
        /// Ends all open sessions.
        /// </summary>
        public static void EndAllSessions()
        {
            foreach (ISession session in SessionStorage.GetAll<ISession>())
            {
                if (session != null && session.IsConnected)
                    session.Close();
            }
        }

        /// <summary>
        /// Gets the database session key for the specified entity type.
        /// </summary>
        /// <param name="entityType"></param>
        /// <returns></returns>
        internal static string GetSessionKey(Type entityType)
        {
            TableMapping mapping = DataMapper.GetTableMapping(entityType);

            return (mapping == null) ? null : mapping.Key;
        }

        #region Transactions

        private static Dictionary<string, Transaction> _transactions = new Dictionary<string, Transaction>();

        /// <summary>
        /// Gets an instance of an active NHibernate transaction.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static Transaction GetTransaction(string key)
        {
            if (_transactions.ContainsKey(key) && _transactions[key].IsActive)
                return _transactions[key];

            ISession session = SessionFactory.GetCurrentSession(key);

            if (_transactions.ContainsKey(key))
                _transactions[key] = new Transaction(session.Connection);
            else
                _transactions.Add(key, new Transaction(session.Connection));

            return _transactions[key];
        }

        /// <summary>
        /// Gets an instance of an active NHibernate transaction.
        /// </summary>
        /// <param name="entityType"></param>
        /// <returns></returns>
        public static Transaction GetTransaction(Type entityType)
        {
            return GetTransaction(GetSessionKey(entityType));
        }

        /// <summary>
        /// Gets an instance of a database transaction for each of the specified keys.
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public static TransactionCollection GetTransactions(IEnumerable<string> keys)
        {
            TransactionCollection transactions = new TransactionCollection();

            foreach (string key in keys)
                transactions.Add(GetTransaction(key));

            return transactions;
        }


        /// <summary>
        /// Gets an instance of a database transaction for each of the specified entity types.
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public static TransactionCollection GetTransactions(IEnumerable<Type> entityTypes)
        {
            TransactionCollection transactions = new TransactionCollection();

            foreach (Type type in entityTypes)
                transactions.Add(GetTransaction(type));

            return transactions;
        }

        #endregion
    }
}
