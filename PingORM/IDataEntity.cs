using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PingORM
{
    public interface IDataEntity
    {
        object GetId();
    }

    public interface IPartitionedEntity
    {
        object GetPartitionKey();
    }
}
