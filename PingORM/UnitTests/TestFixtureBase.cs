using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Configuration;
using System.Reflection;

namespace PingORM.UnitTests
{
    /// <summary>
    /// Base class for test fixtures.
    /// </summary>
    public class TestFixtureBase
    {
        static log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// Whether or not the system has been initialized (so we don't re-initialize it for every test).
        /// </summary>
        protected static bool IsSystemInitialized { get; set; }

        /// <summary>
        /// The entity tracker instance that tracks any database entities created by the unit tests and deletes them when the test is done.
        /// </summary>
        protected static TestEntityTracker EntityTracker;

        /// <summary>
        /// The assembly containing any custom entity adapters.
        /// </summary>
        protected virtual Assembly AdaptersAssembly { get { return null; } }

        /// <summary>
        /// The list of data entities in the order that they should be deleted based on the reference hierarchy.
        /// </summary>
        protected virtual List<string> EntityHierarchyList { get { return new List<string>(); } }

        /// <summary>
        /// Sets up the environment needed to run the tests.
        /// </summary>
        [TestFixtureSetUp]
        public virtual void TestFixtureSetup()
        {
            if (!IsSystemInitialized)
            {
                log4net.Config.XmlConfigurator.Configure();
                Log.Debug("Start Logging");

                // Fluently create the session factory using the connection data in the configuration file.
                SessionFactory.Initialize();

                // Set the hierarchy of the entities in the entity tracker.
                TestEntityTracker.SetEntityHierarchy(this.EntityHierarchyList);

                EntityTracker = new TestEntityTracker();
                EntityAdapter.SetEntityTracker(EntityTracker);

                // Register any custom entity adapter classes.
                if (AdaptersAssembly != null)
                    EntityAdapter.RegisterAdapters(AdaptersAssembly);

                IsSystemInitialized = true;
            }
        }

        /// <summary>
        /// Setup that runs before each individual test method.
        /// </summary>
        [SetUp]
        public virtual void TestSetup()
        {

        }

        /// <summary>
        /// Clean-up code that runs after each test completes.
        /// </summary>
        [TearDown]
        public virtual void Teardown()
        {
            EntityTracker.CleanUp();
        }

        /// <summary>
        /// Clean-up code that runs after each test fixture completes.
        /// </summary>
        [TestFixtureTearDown]
        public virtual void FixtureTeardown()
        {

        }
    }
}
