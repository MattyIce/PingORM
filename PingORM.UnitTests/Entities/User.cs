using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PingORM.UnitTests.Entities
{
    [DataEntity(Key = "tests", TableName = "users", SequenceName = "seq_users")]
    public class User
    {
        [Column(Name = "id", IsPrimaryKey = true)]
        public long Id { get; set; }

        [Column(Name = "first_name")]
        public string FirstName { get; set; }

        [Column(Name = "last_name")]
        public string LastName { get; set; }

        [Column(Name = "join_date")]
        public DateTime JoinDate { get; set; }

        [Column(Name = "num_logins")]
        public int NumLogins { get; set; }
    }
}
