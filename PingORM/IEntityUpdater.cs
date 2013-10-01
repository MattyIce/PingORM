using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PingORM
{
    public interface IEntityUpdater<in ENTITY> where ENTITY : class, new()
    {
        int Insert(ENTITY entity);
        void Update(ENTITY entity);
        void Delete(ENTITY entity);
    }
}
