using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using PingORM.UnitTests.Entities;

namespace PingORM.UnitTests
{
    /// <summary>
    /// Tests compiled query functionality.
    /// </summary>
    [TestFixture]
    public class CompiledQueryTestFixture : TestFixtureBase
    {
        [Test]
        public void SelectTest()
        {
            EntityAdapter.Insert(new User { FirstName = "Matt", LastName = "Rosen", JoinDate = DateTime.Now });

            List<User> users = EntityAdapter.Query<User>().ToList();

            Assert.AreEqual(1, users.Count);
        }

        public static Func<long, int, QueryBuilder<User>> test = QueryBuilder<User>.Compile<long, int>(
            (id, amount) => EntityAdapter.Query<User>()
                .Where(p => p.Id == id)
                .Update(p => p.NumLogins, p => p.NumLogins + amount));

        [Test]
        public void UpdateTest()
        {
            User user = EntityAdapter.Insert(new User { FirstName = "Matt", LastName = "Rosen", JoinDate = DateTime.Now, NumLogins = 2 });

            int count = test(user.Id, 4).ExecuteNonQuery();

            Assert.AreEqual(1, count);
            Assert.AreEqual(user.NumLogins + 4, EntityAdapter.Get<User>(user.Id).NumLogins);
        }
    }
}
