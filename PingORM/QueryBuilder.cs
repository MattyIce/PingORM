using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Data;
using System.Reflection;
using System.Collections;
using System.Dynamic;

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
        public Dictionary<string, QueryParameter> Parameters = new Dictionary<string, QueryParameter>();
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

        public virtual QueryBuilder<T> Update<TKey>(Expression<Func<T, TKey>> keySelector, Expression<Func<T, TKey>> expr)
        {
            return Update(((MemberExpression)keySelector.Body).Member.Name, expr);
        }

        public virtual QueryBuilder<T> Update<TKey>(string propertyName, Expression<Func<T, TKey>> expr)
        {
            this.isUpdate = true;

            if (this.UpdateStr.Length > 0)
                this.UpdateStr.Append(", ");

            this.UpdateStr.Append(String.Format("{0} = ", DataMapper.EscapeName(DataMapper.GetColumnName(typeof(T), propertyName))));

            this.Visit(expr, this.UpdateStr);

            return this;
        }

        public virtual QueryBuilder<T> Update(string propertyName, object value)
        {
            this.isUpdate = true;

            if (this.UpdateStr.Length > 0)
                this.UpdateStr.Append(", ");

            this.UpdateStr.Append(String.Format("{0} = ", DataMapper.EscapeName(DataMapper.GetColumnName(typeof(T), propertyName))));

            this.AddParameter(this.UpdateStr, null, value, value.GetType());

            return this;
        }

        public virtual int ExecuteNonQuery()
        {
            return DataMapper.NonQuery(SessionFactory.GetCurrentSession(typeof(T)), this);
        }

        public virtual int Count()
        {
            this.SelectStr = String.Format("SELECT COUNT(*) FROM {0}", DataMapper.EscapeName(Mapping.TableName));
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
            StringBuilder query = new StringBuilder((this.isUpdate) ? String.Format("UPDATE {0}", DataMapper.EscapeName(this.Mapping.TableName)) : this.SelectStr);

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

            this.OrderByStr.Append(DataMapper.EscapeName(DataMapper.GetColumnName(typeof(T), ((MemberExpression)keySelector.Body).Member.Name)));
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
            if (value is IEnumerable && value.GetType() != typeof(String))
            {
                StringBuilder sb = new StringBuilder();
                int i = 0;
                foreach (object val in (IEnumerable)value)
                {
                    string paramName = DataMapper.ParamName(String.Format("{0}{1}", name, i++));

                    if (i > 1)
                        sb.Append(",");

                    sb.Append(paramName);

                    // Add or replace the new parameter.
                    if (Parameters.ContainsKey(paramName))
                        Parameters[paramName].Value = val;
                    else
                        Parameters.Add(paramName, new QueryParameter { Value = val });
                }

                string baseParamName = DataMapper.ParamName(name);

                if (Parameters.ContainsKey(baseParamName))
                {
                    // Replace the single parameter in the WHERE and UPDATE clauses with the parameter list.
                    this.WhereStr.Replace(baseParamName, sb.ToString());
                    this.UpdateStr.Replace(baseParamName, sb.ToString());

                    // Remove the single placeholder parameter.
                    Parameters.Remove(baseParamName);
                }
            }
            else
            {
                string paramName = DataMapper.ParamName(name);

                if (Parameters.ContainsKey(paramName))
                {
                    QueryParameter parameter = Parameters[paramName];

                    if (value != null && !String.IsNullOrEmpty(parameter.AppendStart) && value.GetType() == typeof(String))
                        value = String.Format("{0}{1}", parameter.AppendStart, value);

                    if (value != null && !String.IsNullOrEmpty(parameter.AppendEnd) && value.GetType() == typeof(String))
                        value = String.Format("{0}{1}", value, parameter.AppendEnd);

                    parameter.Value = value;
                }
            }

            return this;
        }

        #endregion Compiled Queries

        #region Expression Parsing

        protected virtual Expression Visit(Expression exp, StringBuilder sb, QueryParameter data = null)
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
                    return this.VisitUnary((UnaryExpression)exp, sb, data);
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
                    return this.VisitBinary((BinaryExpression)exp, sb, data);
                case ExpressionType.Constant:
                    return this.VisitConstant((ConstantExpression)exp, sb, data);
                case ExpressionType.MemberAccess:
                    return this.VisitMemberAccess((MemberExpression)exp, sb, data);
                case ExpressionType.Lambda:
                    return this.VisitLambda((LambdaExpression)exp, sb, data);
                case ExpressionType.Call:
                    return this.VisitMethodCall((MethodCallExpression)exp, sb, data);
                case ExpressionType.NewArrayInit:
                case ExpressionType.ListInit:
                    return this.VisitConstant(Expression.Constant(Expression.Lambda(exp).Compile().DynamicInvoke(null)), sb, data);
                default:
                    throw new Exception(string.Format("Unhandled expression type: '{0}'", exp.NodeType));
            }
        }

        protected virtual Expression VisitMethodCall(MethodCallExpression exp, StringBuilder sb, QueryParameter data = null)
        {
            switch (exp.Method.Name)
            {
                case "Contains":
                    if (exp.Object != null && exp.Object.Type == typeof(String))
                    {
                        // String contains
                        this.Visit(exp.Object, sb, data);
                        sb.Append(" ILIKE ");
                        this.Visit(exp.Arguments[0], sb, new QueryParameter { AppendStart = "%", AppendEnd = "%" });
                    }
                    else
                    {
                        // List contains
                        this.Visit((exp.Object == null) ? exp.Arguments[1] : exp.Arguments[0], sb, data);
                        sb.Append(" IN ");
                        this.Visit((exp.Object == null) ? exp.Arguments[0] : exp.Object, sb, data);
                    }
                    return exp;
                case "StartsWith":
                    if (exp.Object != null && exp.Object.Type == typeof(String))
                    {
                        this.Visit(exp.Object, sb, data);
                        sb.Append(" ILIKE ");
                        this.Visit(exp.Arguments[0], sb, new QueryParameter { AppendEnd = "%" });
                    }
                    else
                        throw new NotSupportedException(String.Format("The method [{0}] is not supported.", exp.Method.Name));

                    return exp;
                default:
                    try
                    {
                        return this.VisitConstant(Expression.Constant(Expression.Lambda(exp).Compile().DynamicInvoke(null)), sb);
                    }
                    catch (Exception) { throw new NotSupportedException(String.Format("The method [{0}] is not supported.", exp.Method.Name)); }
            }
        }

        protected virtual Expression VisitUnary(UnaryExpression u, StringBuilder sb, QueryParameter data = null)
        {
            switch (u.NodeType)
            {
                case ExpressionType.Not:
                    sb.Append(" NOT ");
                    this.Visit(u.Operand, sb, data);
                    break;
                case ExpressionType.Convert:
                    if (u.Operand.NodeType == ExpressionType.MemberAccess)
                        this.Visit(u.Operand, sb, data);
                    else
                        this.VisitConstant(Expression.Constant(Expression.Lambda(u).Compile().DynamicInvoke(null)), sb, data);
                    break;
                default:
                    throw new NotSupportedException(string.Format("The unary operator '{0}' is not supported", u.NodeType));
            }
            return u;
        }

        protected virtual Expression VisitBinary(BinaryExpression b, StringBuilder sb, QueryParameter data = null)
        {
            sb.Append("(");
            this.Visit(b.Left, sb, data);
            switch (b.NodeType)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    sb.Append(" AND ");
                    break;
                case ExpressionType.Or:
                case ExpressionType.OrElse:
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
                case ExpressionType.Add:
                    sb.Append(" + ");
                    break;
                case ExpressionType.Subtract:
                    sb.Append(" - ");
                    break;
                default:
                    throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", b.NodeType));
            }
            this.Visit(b.Right, sb, data);
            sb.Append(")");
            return b;
        }

        protected virtual Expression VisitConstant(ConstantExpression c, StringBuilder sb, QueryParameter data = null)
        {
            AddParameter(sb, null, c.Value, c.Value.GetType(), data);
            return c;
        }

        protected virtual Expression VisitMemberAccess(MemberExpression m, StringBuilder sb, QueryParameter data = null)
        {
            if (m.Expression == null)
            {
                this.VisitConstant(Expression.Constant(Expression.Lambda(m).Compile().DynamicInvoke(null)), sb, data);
                return m;
            }

            switch(m.Expression.NodeType)
            {
                case ExpressionType.Parameter:
                    sb.Append(DataMapper.EscapeName(DataMapper.GetColumnName(typeof(T), m.Member.Name)));
                    break;
                case ExpressionType.Constant:
                    if(m.Member.MemberType == MemberTypes.Field)
                        AddParameter(sb, m.Member.Name, ((FieldInfo)m.Member).GetValue(((ConstantExpression)m.Expression).Value), ((FieldInfo)m.Member).FieldType, data);
                    else
                        AddParameter(sb, m.Member.Name, ((PropertyInfo)m.Member).GetValue(((ConstantExpression)m.Expression).Value), ((PropertyInfo)m.Member).PropertyType, data);
                    break;
                case ExpressionType.MemberAccess:
                    this.VisitConstant(Expression.Constant(Expression.Lambda(m).Compile().DynamicInvoke(null)), sb, data);
                    break;
                default:
                    throw new NotSupportedException(string.Format("The member '{0}' is not supported", m.Member.Name));
            }
            return m;
        }

        protected virtual Expression VisitLambda(LambdaExpression lambda, StringBuilder sb, QueryParameter data = null)
        {
            Expression body = this.Visit(lambda.Body, sb, data);

            if (body != lambda.Body)
                return Expression.Lambda(lambda.Type, body, lambda.Parameters);
            
            return lambda;
        }

        protected virtual void AddParameter(StringBuilder sb, string name, object value, Type type, QueryParameter data = null)
        {
            // Is this an enumerable type that needs to be split into separate variables?
            bool isEnumerable = typeof(IEnumerable).IsAssignableFrom(type) && type != typeof(String);

            if (isEnumerable)
                sb.Append("(");

            if (isEnumerable && value != null)
            {
                // If the value is enumerable
                int i = 0;
                foreach (object val in (IEnumerable)value)
                {
                    string paramName = String.IsNullOrEmpty(name) ? DataMapper.ParamName(Parameters.Count, true) : DataMapper.ParamName(String.Format("{0}{1}", name, i++));

                    if (i > 1)
                        sb.Append(",");

                    sb.Append(paramName);
                    Parameters.Add(paramName, new QueryParameter { Value = val });
                }
            }
            else
            {
                if (data != null && value != null && !String.IsNullOrEmpty(data.AppendStart) && value.GetType() == typeof(String))
                    value = String.Format("{0}{1}", data.AppendStart, value);

                if (data != null && value != null && !String.IsNullOrEmpty(data.AppendEnd) && value.GetType() == typeof(String))
                    value = String.Format("{0}{1}", value, data.AppendEnd);

                string paramName = String.IsNullOrEmpty(name) ? DataMapper.ParamName(Parameters.Count, true) : DataMapper.ParamName(name);
                sb.Append(paramName);

                if (data == null)
                    data = new QueryParameter { Value = value };
                else
                    data.Value = value;

                Parameters.Add(paramName, data);
            }

            if (isEnumerable)
                sb.Append(")");
        }

        #endregion Expression Parsing

        public class QueryParameter
        {
            public object Value { get; set; }
            public string AppendStart { get; set; }
            public string AppendEnd { get; set; }
        }
    }
}
