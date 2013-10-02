using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using Npgsql;
using MySql.Data.MySqlClient;

namespace PingORM
{
    public class DataSession : ISession
    {
        public IDbConnection Connection { get; set; }

        public bool IsConnected { get { return Connection != null && Connection.State != System.Data.ConnectionState.Closed; } }

        public DataSession(string connectionString)
        {
            Connection = new MySqlConnection(connectionString);
            Connection.Open();
        }

        public void Close()
        {
            if (Connection != null && Connection.State != System.Data.ConnectionState.Closed)
                Connection.Close();

            Connection = null;
        }

        public void Dispose()
        {
            if (Connection != null && Connection.State != System.Data.ConnectionState.Closed)
                Connection.Close();

            Connection = null;
        }
    }
}
