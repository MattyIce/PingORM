using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;

namespace PingORM
{
    /// <summary>
    /// Interface for a class that stores an NHibernate session.
    /// </summary>
    public interface IKeyStorage<T> where T : class
    {
        /// <summary>
        /// Gets the currently stored item for the specified key.
        /// </summary>
        /// <param name="key"></param>
        T GetCurrent(string key);

        /// <summary>
        /// Stores the current item for the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="session"></param>
        void SetCurrent(string key, T item);
    }

    /// <summary>
    /// Class that stores an NHibernate session in a static variable.
    /// </summary>
    public class StaticKeyStorage<T> : IKeyStorage<T> where T : class
    {
        /// <summary>
        /// The list of sessions associated with the available connection keys.
        /// </summary>
        protected static Dictionary<string, T> _currentItems = new Dictionary<string, T>();

        /// <summary>
        /// Gets the current session for the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public T GetCurrent(string key)
        {
            return _currentItems.ContainsKey(key) ? _currentItems[key] : null;
        }

        /// <summary>
        /// Stores the current session for the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="session"></param>
        public void SetCurrent(string key, T item)
        {
            if (_currentItems.ContainsKey(key))
                _currentItems[key] = item;
            else
                _currentItems.Add(key, item);
        }
    }

    /// <summary>
    /// Stores a thread-static session.
    /// </summary>
    public class ThreadKeyStorage<T> : IKeyStorage<T> where T : class
    {
        /// <summary>
        /// The list of sessions associated with the available connection keys.
        /// </summary>
        [ThreadStatic]
        protected static Dictionary<string, T> _currentItems = new Dictionary<string, T>();

        /// <summary>
        /// Gets the current session for the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public T GetCurrent(string key)
        {
            return _currentItems.ContainsKey(key) ? _currentItems[key] : null;
        }

        /// <summary>
        /// Stores the current session for the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="session"></param>
        public void SetCurrent(string key, T item)
        {
            if (_currentItems.ContainsKey(key))
                _currentItems[key] = item;
            else
                _currentItems.Add(key, item);
        }
    }

    public class WebKeyStorage<T> : IKeyStorage<T> where T : class
    {
        public T GetCurrent(string key)
        {
            return HttpContext.Current.Items[String.Format("{0}_{1}", typeof(T).Name, key)] as T;
        }

        public void SetCurrent(string key, T item)
        {
            HttpContext.Current.Items[String.Format("{0}_{1}", typeof(T).Name, key)] = item;
        }
    }
}
