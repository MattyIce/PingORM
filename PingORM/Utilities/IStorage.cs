using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PingORM.Utilities
{
    /// <summary>
    /// Interface for a class that stores an item.
    /// </summary>
    /// <typeparam name="T">The type of item being stored.</typeparam>
    public interface IStorage<T> where T : class
    {
        /// <summary>
        /// The currently stored item.
        /// </summary>
        T Current { get; set; }
    }

    /// <summary>
    /// Class that stores an item in a static variable.
    /// </summary>
    public class StaticStorage<T> : IStorage<T> where T : class
    {
        private static T _current;

        /// <summary>
        /// The currently stored item.
        /// </summary>
        public T Current
        {
            get { return _current; }
            set { _current = value; }
        }
    }

    /// <summary>
    /// Class that stores an item in a thread-static variable.
    /// </summary>
    public class ThreadStorage<T> : IStorage<T> where T : class
    {
        [ThreadStatic]
        private static T _current;

        /// <summary>
        /// The currently stored item.
        /// </summary>
        public T Current
        {
            get { return _current; }
            set { _current = value; }
        }
    }
}
