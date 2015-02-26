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
        QueryBuilder<ENTITY> Query();
        ENTITY Insert(ENTITY entity);
        void Update(ENTITY entity);
        void Delete(ENTITY entity);
    }

    /*
     * I've decided not to allow this since it will allow anyone to easily modify entities without using the adapter specific to that entity.
     * For example, say I make a custom entity adapter for the User object called NoMattsEntityAdapter<User> which has custom code to check if a user's name is "Matt" and 
     * not let them in and I register that in the IoC to be what is used for any instances of IEntityAdapter<User>. Now if I allow this base IEntityAdapter interface/class
     * to be used then people could register a user named "Matt" and get around the check that was implemented.
     * 
    public interface IEntityAdapter
    {
        ENTITY Get<ENTITY>(object id, bool forUpdate = false) where ENTITY : class, new();
        object Get(string entityName, object id);
        object Get(Type entityType, object id);
        QueryBuilder<ENTITY> Query<ENTITY>() where ENTITY : class, new();
        ENTITY Insert<ENTITY>(ENTITY entity) where ENTITY : class, new();
        void Update<ENTITY>(ENTITY entity) where ENTITY : class, new();
        void Delete<ENTITY>(ENTITY entity) where ENTITY : class, new();
    }
     */
}
