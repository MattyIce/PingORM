using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PingORM
{
    public interface IEntityAdapter<ENTITY> where ENTITY : class, new()
    {
        ENTITY Get(object id, bool forUpdate = false);
        object Get(string entityName, object id);
        object Get(Type entityType, object id);
        QueryBuilder<ENTITY> Query();
        ENTITY Insert(ENTITY entity);
        void Update(ENTITY entity);
        void Delete(ENTITY entity);

        void SetEntityTracker(IEntityTracker entityTracker);
    }
}
