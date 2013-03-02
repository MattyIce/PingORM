using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using PingORM;

namespace PingORM
{
    public abstract class DataEntityBase<ID> : DataEntityBase where ID : struct
    {
        /// <summary>
        /// The id of this entity instance.
        /// </summary>
        public virtual ID Id
        {
            get { return (GetId() == null) ? default(ID) : (ID)GetId(); }
            set { _id = value; }
        }
    }

    public abstract class CompositeKeyEntityBase<ID> : DataEntityBase<ID> where ID : struct
    {
        /// <summary>
        /// The id of this entity instance.
        /// </summary>
        public override ID Id
        {
            get { return base.Id; }
            set { base.Id = value; }
        }
    }

    public abstract class DataEntityBase : IDataEntity, IEquatable<DataEntityBase>
    {
        /// <summary>
        /// The id of this entity instance.
        /// </summary>
        protected object _id { get; set; }

        /// <summary>
        /// Gets a generic object version of this entity's identifier.
        /// </summary>
        /// <returns></returns>
        public virtual object GetId() { return _id; }

        /// <summary>
        /// Tests if this entity is equal to another entity by comparing their type and Ids.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj) { return Equals(obj as DataEntityBase); }

        /// <summary>
        /// Tests if this entity is equal to another entity by comparing their Ids.
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public virtual bool Equals(DataEntityBase entity)
        {
            if (entity == null)
                return false;

            return (this.GetType() == entity.GetType() && entity.GetId().Equals(this.GetId()));
        }

        /// <summary>
        /// Override the == operator to test equality by comparing the Id.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static bool operator ==(DataEntityBase a, DataEntityBase b)
        {
            // If both are null, or both are same instance, return true.
            if (System.Object.ReferenceEquals(a, b))
                return true;

            // If one is null, but not both, return false.
            if (((object)a == null) || ((object)b == null))
                return false;

            // Return true if the Ids match:
            return a.Equals(b);
        }

        /// <summary>
        /// Override the != operator to test inequality by comparing the Id.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static bool operator !=(DataEntityBase a, DataEntityBase b)
        {
            return !(a == b);
        }

        /// <summary>
        /// Gets a hash code for this entity based on its Id.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode() { return this.GetId().GetHashCode(); }
    }
}
