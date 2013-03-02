using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace PingORM
{
    public interface ISession : IDisposable
    {
        IDbConnection Connection { get; set; }
        bool IsConnected { get; }
        void Close();
    }
}
