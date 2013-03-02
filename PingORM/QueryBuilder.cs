using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Data;
using System.Reflection;
using System.Collections;

namespace PingORM
{
    public class QueryBuilder<T> : IEnumerable<T> where T : class, new()
    {
        protected int Limit { get; set; }
        protected int SkipRows { get; set; }
        protected string SelectStr { get; set; }
        protected StringBuilder WhereStr = new StringBuilder();
        protected StringBuilder OrderByStr = new StringBuilder();
        protected StringBuilder UpdateStr = new StringBuilder();
        public Dictionary<string, object> Parameters = new Dictionary<string, object>();
        internal TableMapping Mapping { get; set; }
        protected bool isUpdate { get; set; }

        public QueryBuilder()
        {
            Mapping = DataMapper.GetTableMapping(typeof(T));
            this.SelectStr = Mapping.SelectExpression;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return DataMapper.Select(SessionFactory.GetCurrentSession(typeof(T)), this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public virtual QueryBuilder<T> Increment<TKey>(Expression<Func<T, TKey>> predicate)
        {
            this.isUpdate = true;

            if (this.UpdateStr.Length > 0)
                this.UpdateStr.Append(", ");

            this.Visit(predicate, UpdateStr);

            //string paramName = String.Format(":u{0}", Parameters.Count);
            //this.UpdateStr.Append(String.Format("\"{0}\" = \"{0}\" + {1}", DataMapper.GetColumnName(typeof(T), ((MemberExpression)keySelector.Body).Member.Name), amount));

            return this;
        }

        public virtual int ExecuteNonQuery()
        {
            return DataMapper.NonQuery(SessionFactory.GetCurrentSession(typeof(T)), this);
        }

        public virtual int Count()
        {
            this.SelectStr = String.Format("SELECT COUNT(*) FROM \"{0}\"", Mapping.TableName);
            return Convert.ToInt32(DataMapper.SelectScalar(SessionFactory.GetCurrentSession(typeof(T)), this));
        }

        public virtual int Count(Expression<Func<T, bool>> predicate)
        {
            return this.Where(predicate).Count();
        }

        public virtual T FirstOrDefault()
        {
            this.Limit = 1;
            List<T> results = this.ToList();
            return (results != null && results.Count > 0) ? results[0] : null;
        }

        public virtual T FirstOrDefault(Expression<Func<T, bool>> predicate)
        {
            return this.Where(predicate).FirstOrDefault();
        }

        public virtual QueryBuilder<T> Skip(int rowsToSkip)
        {
            this.SkipRows = rowsToSkip;
            return this;
        }

        public virtual QueryBuilder<T> Take(int maxRows)
        {
            this.Limit = maxRows;
            return this;
        }

        public override string ToString()
        {
            StringBuilder query = new StringBuilder((this.isUpdate) ? String.Format("UPDATE \"{0}\"", this.Mapping.TableName) : this.SelectStr);

            if (this.UpdateStr.Length > 0)
                query.Append(" SET ").Append(this.UpdateStr.ToString());

            if (this.WhereStr.Length > 0)
                query.Append(" WHERE ").Append(this.WhereStr.ToString());

            if (this.OrderByStr.Length > 0)
                query.Append(" ORDER BY ").Append(this.OrderByStr.ToString());

            if (this.SkipRows > 0)
                query.Append(" OFFSET ").Append(this.SkipRows);

            if (this.Limit > 0)
                query.Append(" LIMIT ").Append(this.Limit);

            return query.ToString();
        }

        public QueryBuilder<T> Where(Expression<Func<T, bool>> predicate)
        {
            if (WhereStr.Length > 0)
                WhereStr.Append(" AND ");

            Visit(predicate, WhereStr);
            return this;
        }

        public QueryBuilder<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            if (this.OrderByStr.Length > 0)
                this.OrderByStr.Append(", ");

            this.OrderByStr.Append(String.Format("\"{0}\"", DataMapper.GetColumnName(typeof(T), ((MemberExpression)keySelector.Body).Member.Name)));
            return this;
        }

        public QueryBuilder<T> OrderByDesc<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            this.OrderBy(keySelector);
            this.OrderByStr.Append(" DESC");
            return this;
        }

        #region Compiled Queries

        /// <summary>
        /// Compile a query with one argument.
        /// </summary>
        /// <typeparam name="Arg0"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static Func<Arg0, QueryBuilder<T>> Compile<Arg0>(Func<Arg0, QueryBuilder<T>> query)
        {
            QueryBuilder<T> compiledQuery = query(default(Arg0));
            return (arg0) => compiledQuery
                .ReplaceParameter(query.Method.GetParameters()[0].Name, arg0);
        }

        /// <summary>
        /// Compile a query with two arguments.
        /// </summary>
        /// <typeparam name="Arg0"></typeparam>
        /// <typeparam name="Arg1"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static Func<Arg0, Arg1, QueryBuilder<T>> Compile<Arg0, Arg1>(Func<Arg0, Arg1, QueryBuilder<T>> query)
        {
            QueryBuilder<T> compiledQuery = query(default(Arg0), default(Arg1));
            ParameterInfo[] parameters = query.Method.GetParameters();
            return (arg0, arg1) => compiledQuery
                .ReplaceParameter(parameters[0].Name, arg0)
                .ReplaceParameter(parameters[1].Name, arg1);
        }

        /// <summary>
        /// Compile a query with three arguments.
        /// </summary>
        /// <typeparam name="Arg0"></typeparam>
        /// <typeparam name="Arg1"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static Func<Arg0, Arg1, Arg2, QueryBuilder<T>> Compile<Arg0, Arg1, Arg2>(Func<Arg0, Arg1, Arg2, QueryBuilder<T>> query)
        {
            QueryBuilder<T> compiledQuery = query(default(Arg0), default(Arg1), default(Arg2));
            ParameterInfo[] parameters = query.Method.GetParameters();
            return (arg0, arg1, arg2) => compiledQuery
                .ReplaceParameter(parameters[0].Name, arg0)
                .ReplaceParameter(parameters[1].Name, arg1)
                .ReplaceParameter(parameters[2].Name, arg2);
        }

        /// <summary>
        /// Compile a query with four arguments.
        /// </summary>
        /// <typeparam name="Arg0"></typeparam>
        /// <typeparam name="Arg1"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static Func<Arg0, Arg1, Arg2, Arg3, QueryBuilder<T>> Compile<Arg0, Arg1, Arg2, Arg3>(Func<Arg0, Arg1, Arg2, Arg3, QueryBuilder<T>> query)
        {
            QueryBuilder<T> compiledQuery = query(default(Arg0), default(Arg1), default(Arg2), default(Arg3));
            ParameterInfo[] parameters = query.Method.GetParameters();
            return (arg0, arg1, arg2, arg3) => compiledQuery
                .ReplaceParameter(parameters[0].Name, arg0)
                .ReplaceParameter(parameters[1].Name, arg1)
                .ReplaceParameter(parameters[2].Name, arg2)
                .ReplaceParameter(parameters[3].Name, arg3);
        }

        /// <summary>
        /// Replace a parameter when a compiled query is executed.
        /// </summary>
        /// <param name="number"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        protected QueryBuilder<T> ReplaceParameter(string name, object value)
        {
            string paramName = String.Format(":p{0}", name);

            if (Parameters.ContainsKey(paramName))
                Parameters[paramName] = value;

            return this;
        }

        #endregion Compiled Queries

        #region Expression Parsing

        protected virtual Expression Visit(Expression exp, StringBuilder sb)
        {
            if (exp == null)
            return exp;
            switch (exp.NodeType) {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                    return this.VisitUnary((UnaryExpression)exp, sb);
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.ArrayIndex:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                    return this.VisitBinary((BinaryExpression)exp, sb);
                case ExpressionType.Constant:
                    return this.VisitConstant((ConstantExpression)exp, sb);
                case ExpressionType.MemberAccess:
                    return this.VisitMemberAccess((MemberExpression)exp, sb);
                case ExpressionType.Lambda:
                    return this.VisitLambda((LambdaExpression)exp, sb);
                case ExpressionType.Call:
                    this.VisitConstant(Expression.Constant(Expression.Lambda(exp).Compile().DynamicInvoke(null)), sb);
                    return exp;
                default:
                    throw new Exception(string.Format("Unhandled expression type: '{0}'", exp.NodeType));
            }
        }

        protected virtual Expression VisitUnary(UnaryExpression u, StringBuilder sb)
        {
            switch (u.NodeType)
            {
                case ExpressionType.Not:
                    sb.Append(" NOT ");
                    this.Visit(u.Operand, sb);
                    break;
                case ExpressionType.Convert:
                    if (u.Operand.NodeType == ExpressionType.MemberAccess)
                        this.Visit(u.Operand, sb);
                    else
                        this.VisitConstant(Expression.Constant(Expression.Lambda(u).Compile().DynamicInvoke(null)), sb);
                    break;
                default:
                    throw new NotSupportedException(string.Format("The unary operator '{0}' is not supported", u.NodeType));
            }
            return u;
        }

        protected virtual Expression VisitBinary(BinaryExpression b, StringBuilder sb)
        {
            sb.Append("(");
            this.Visit(b.Left, sb);
            switch (b.NodeType)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    sb.Append(" AND ");
                    break;
                case ExpressionType.Or:
                    sb.Append(" OR");
                    break;
                case ExpressionType.Equal:
                    sb.Append(" = ");
                    break;
                case ExpressionType.NotEqual:
                    sb.Append(" <> ");
                    break;
                case ExpressionType.LessThan:
                    sb.Append(" < ");
                    break;
                case ExpressionType.LessThanOrEqual:
                    sb.Append(" <= ");
                    break;
                case ExpressionType.GreaterThan:
                    sb.Append(" > ");
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    sb.Append(" >= ");
                    break;
                case ExpressionType.Multiply:
                    sb.Append(" * ");
                    break;
                default:
                    throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", b.NodeType));
            }
            this.Visit(b.Right, sb);
            sb.Append(")");
            return b;
        }

        protected virtual Expression VisitConstant(ConstantExpression c, StringBuilder sb)
        {
            string paramName = String.Format(":c{0}", Parameters.Count);
            sb.Append(paramName);
            Parameters.Add(paramName, c.Value);
            return c;
        }

        protected virtual Expression VisitMemberAccess(MemberExpression m, StringBuilder sb)
        {
            if (m.Expression == null)
            {
                this.VisitConstant(Expression.Constant(Expression.Lambda(m).Compile().DynamicInvoke(null)), sb);
                return m;
            }

            switch(m.Expression.NodeType)
            {
                case ExpressionType.Parameter:
                    sb.Append(String.Format("\"{0}\"", DataMapper.GetColumnName(typeof(T), m.Member.Name)));
                    break;
                case ExpressionType.Constant:
                    string paramName = String.Format(":p{0}", m.Member.Name);
                    sb.Append(paramName);
                    Parameters.Add(paramName, ((FieldInfo)m.Member).GetValue(((ConstantExpression)m.Expression).Value));
                    break;
                case ExpressionType.MemberAccess:
                    this.VisitConstant(Expression.Constant(Expression.Lambda(m).Compile().DynamicInvoke(null)), sb);
                    break;
                default:
                    throw new NotSupportedException(string.Format("The member '{0}' is not supported", m.Member.Name));
            }
            return m;
        }

        protected virtual Expression VisitLambda(LambdaExpression lambda, StringBuilder sb)
        {
            Expression body = this.Visit(lambda.Body, sb);
            if (body != lambda.Body)
            {
                return Expression.Lambda(lambda.Type, body, lambda.Parameters);
            }
            return lambda;
        }

        #endregion Expression Parsing
    }
}
