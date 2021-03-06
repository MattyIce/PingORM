﻿using System;
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

        public static Func<long, int, QueryBuilder<User>> updateTestQuery = QueryBuilder<User>.Compile<long, int>(
            (id, amount) => EntityAdapter.Query<User>()
                .Where(p => p.Id == id)
                .Update(p => p.NumLogins, p => p.NumLogins + amount));

        [Test]
        public void UpdateTest()
        {
            User user = EntityAdapter.Insert(new User { FirstName = "Matt", LastName = "Rosen", JoinDate = DateTime.Now, NumLogins = 2 });

            int count = updateTestQuery(user.Id, 4).ExecuteNonQuery();

            Assert.AreEqual(1, count);
            Assert.AreEqual(user.NumLogins + 4, EntityAdapter.Get<User>(user.Id).NumLogins);
        }

        public static Func<long[], QueryBuilder<User>> listContainsTestQuery = QueryBuilder<User>.Compile<long[]>(
            (ids) => EntityAdapter.Query<User>().Where(u => ids.Contains(u.Id)).OrderBy(u => u.Id));

        [Test]
        public void ListContainsTest()
        {
            User user = EntityAdapter.Insert(new User { FirstName = "Eric", LastName = "Cartman", JoinDate = DateTime.Now, NumLogins = 2 });
            User user2 = EntityAdapter.Insert(new User { FirstName = "Stan", LastName = "Marsh", JoinDate = DateTime.Now, NumLogins = 1 });
            User user3 = EntityAdapter.Insert(new User { FirstName = "Kyle", LastName = "Broflovsky", JoinDate = DateTime.Now, NumLogins = 3 });
            User user4 = EntityAdapter.Insert(new User { FirstName = "Kenny", LastName = "McCormick", JoinDate = DateTime.Now, NumLogins = 0 });

            List<User> users = listContainsTestQuery(new long[] { user2.Id, user4.Id }).ToList();

            Assert.AreEqual(2, users.Count);
            Assert.AreEqual(user2.FirstName, users[0].FirstName);
            Assert.AreEqual(user4.FirstName, users[1].FirstName);
        }

        public static Func<string, QueryBuilder<User>> stringContainsTestQuery = QueryBuilder<User>.Compile<string>(
            (str) => EntityAdapter.Query<User>().Where(u => u.LastName.Contains(str)));

        [Test]
        public void StringContainsTest()
        {
            User user = EntityAdapter.Insert(new User { FirstName = "Matt", LastName = "Rosen", JoinDate = DateTime.Now, NumLogins = 2 });
            User user2 = EntityAdapter.Insert(new User { FirstName = "Dan", LastName = "Cohen", JoinDate = DateTime.Now, NumLogins = 3 });
            User user3 = EntityAdapter.Insert(new User { FirstName = "Adam", LastName = "Rosenthal", JoinDate = DateTime.Now, NumLogins = 1 });
            User user4 = EntityAdapter.Insert(new User { FirstName = "Bob", LastName = "Schmarosenthal", JoinDate = DateTime.Now, NumLogins = 1 });

            List<User> users = stringContainsTestQuery("Rosen").ToList();

            Assert.AreEqual(3, users.Count);
            Assert.IsTrue(users[0].LastName.ToLower().Contains("rosen"));
            Assert.IsTrue(users[1].LastName.ToLower().Contains("rosen"));
            Assert.IsTrue(users[2].LastName.ToLower().Contains("rosen"));
        }

        public static Func<string, QueryBuilder<User>> stringStartsWithTestQuery = QueryBuilder<User>.Compile<string>(
            (str) => EntityAdapter.Query<User>().Where(u => u.LastName.StartsWith(str)));

        [Test]
        public void StringStartsWithTest()
        {
            User user = EntityAdapter.Insert(new User { FirstName = "Matt", LastName = "rosen", JoinDate = DateTime.Now, NumLogins = 2 });
            User user2 = EntityAdapter.Insert(new User { FirstName = "Dan", LastName = "Cohen", JoinDate = DateTime.Now, NumLogins = 3 });
            User user3 = EntityAdapter.Insert(new User { FirstName = "Adam", LastName = "Rosenthal", JoinDate = DateTime.Now, NumLogins = 1 });
            User user4 = EntityAdapter.Insert(new User { FirstName = "Bob", LastName = "Schmarosenthal", JoinDate = DateTime.Now, NumLogins = 1 });

            List<User> users = stringStartsWithTestQuery("Rosen").ToList();

            Assert.AreEqual(2, users.Count);
            Assert.IsTrue(users[0].LastName.ToLower().StartsWith("rosen"));
            Assert.IsTrue(users[1].LastName.ToLower().StartsWith("rosen"));
        }
    }
}
