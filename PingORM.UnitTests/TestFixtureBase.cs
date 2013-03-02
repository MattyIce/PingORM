using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace PingORM.UnitTests
{
    public class TestFixtureBase
    {
        static log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// Whether or not the system has been initialized (so we don't re-initialize it for every test).
        /// </summary>
        protected static bool IsSystemInitialized { get; set; }
        /// <summary>
        /// Sets up the environment needed to run the tests.
        /// </summary>
        [TestFixtureSetUp]
        public virtual void Setup()
        {
            if (!IsSystemInitialized)
            {
                log4net.Config.XmlConfigurator.Configure();
                Log.Debug("Start Logging");

                // Create the session factory using the connection data in the configuration file.
                SessionFactory.Initialize();

                IsSystemInitialized = true;
            }
        }

        /// <summary>
        /// Clean-up code that runs after each test completes.
        /// </summary>
        [TearDown]
        public virtual void Teardown()
        {
            
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
