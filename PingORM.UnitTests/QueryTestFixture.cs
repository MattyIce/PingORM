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
    public class QueryTestFixture : TestFixtureBase
    {        
        [Test]
        public void SelectTest()
        {
            EntityAdapter.Insert(new User { FirstName = "Matt", LastName = "Rosen", JoinDate = DateTime.Now });

            List<User> users = EntityAdapter.Query<User>().ToList();

            Assert.AreEqual(1, users.Count);
        }

        [Test]
        public void UpdateTest()
        {
            User user = EntityAdapter.Insert(new User { FirstName = "Matt", LastName = "Rosen", JoinDate = DateTime.Now, NumLogins = 2 }) as User;

            int count = EntityAdapter.Query<User>().
                Where(p => p.Id == user.Id)
                .Update(p => p.NumLogins, p => p.NumLogins + 4).ExecuteNonQuery();

            Assert.AreEqual(1, count);
            Assert.AreEqual(user.NumLogins + 4, EntityAdapter.Get<User>(user.Id).NumLogins);
        }

        [Test]
        public void InTest()
        {
            User user = EntityAdapter.Insert(new User { FirstName = "Eric", LastName = "Cartman", JoinDate = DateTime.Now, NumLogins = 2 }) as User;
            User user2 = EntityAdapter.Insert(new User { FirstName = "Stan", LastName = "Marsh", JoinDate = DateTime.Now, NumLogins = 1 }) as User;
            User user3 = EntityAdapter.Insert(new User { FirstName = "Kyle", LastName = "Broflovsky", JoinDate = DateTime.Now, NumLogins = 3 }) as User;
            User user4 = EntityAdapter.Insert(new User { FirstName = "Kenny", LastName = "McCormick", JoinDate = DateTime.Now, NumLogins = 0 }) as User;
            
            long[] ids = { user2.Id, user4.Id };
            List<User> users = EntityAdapter.Query<User>().Where(u => ids.Contains(u.Id)).OrderBy(u => u.Id).ToList();

            Assert.AreEqual(2, users.Count);
            Assert.AreEqual(user2.FirstName, users[0].FirstName);
            Assert.AreEqual(user4.FirstName, users[1].FirstName);
        }
    }
}
