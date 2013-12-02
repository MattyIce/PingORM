using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;

namespace PingORM.Utilities
{
    /// <summary>
    /// Interface for a class that stores an NHibernate session.
    /// </summary>
    public interface IKeyStorage
    {
        /// <summary>
        /// Gets the currently stored item for the specified key.
        /// </summary>
        /// <param name="key"></param>
        T GetCurrent<T>(object key) where T : class;

        /// <summary>
        /// Stores the current item for the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="session"></param>
        void SetCurrent(object key, object item);

        /// <summary>
        /// Gets all of the items stored.
        /// </summary>
        /// <returns></returns>
        List<T> GetAll<T>() where T : class;
    }

    /// <summary>
    /// Class that stores an NHibernate session in a static variable.
    /// </summary>
    public class StaticKeyStorage : IKeyStorage
    {
        /// <summary>
        /// The list of sessions associated with the available connection keys.
        /// </summary>
        protected static Dictionary<object, object> _currentItems = new Dictionary<object, object>();

        /// <summary>
        /// Gets the current session for the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public T GetCurrent<T>(object key) where T : class
        {
            return _currentItems.ContainsKey(key) ? _currentItems[key] as T : null;
        }

        /// <summary>
        /// Stores the current session for the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="session"></param>
        public void SetCurrent(object key, object item)
        {
            if (_currentItems.ContainsKey(key))
                _currentItems[key] = item;
            else
                _currentItems.Add(key, item);
        }

        /// <summary>
        /// Gets all of the items stored.
        /// </summary>
        /// <returns></returns>
        public List<T> GetAll<T>() where T : class
        {
            return _currentItems.Values.Cast<T>().ToList();
        }
    }

    /// <summary>
    /// Stores a thread-static session.
    /// </summary>
    public class ThreadKeyStorage : IKeyStorage
    {
        /// <summary>
        /// The list of sessions associated with the available connection keys.
        /// </summary>
        [ThreadStatic]
        protected static Dictionary<object, object> _currentItems = new Dictionary<object, object>();

        /// <summary>
        /// Gets the current session for the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public T GetCurrent<T>(object key) where T : class
        {
            return _currentItems.ContainsKey(key) ? _currentItems[key] as T : null;
        }

        /// <summary>
        /// Stores the current session for the specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="session"></param>
        public void SetCurrent(object key, object item)
        {
            if (_currentItems.ContainsKey(key))
                _currentItems[key] = item;
            else
                _currentItems.Add(key, item);
        }

        /// <summary>
        /// Gets all of the items stored.
        /// </summary>
        /// <returns></returns>
        public List<T> GetAll<T>() where T : class
        {
            return _currentItems.Values.Cast<T>().ToList();
        }
    }

    public class WebKeyStorage : IKeyStorage
    {
        public T GetCurrent<T>(object key) where T : class
        {
            return HttpContext.Current.Items[String.Format("CACHE_{0}", key.ToString())] as T;
        }

        public void SetCurrent(object key, object item)
        {
            HttpContext.Current.Items[String.Format("CACHE_{0}", key.ToString())] = item;
        }

        /// <summary>
        /// Gets all of the items stored.
        /// </summary>
        /// <returns></returns>
        public List<T> GetAll<T>() where T : class
        {
            List<T> items = new List<T>();
            foreach (object value in HttpContext.Current.Items.Values)
            {
                if (value as T != null)
                    items.Add(value as T);
            }
            return items;
        }
    }
}
