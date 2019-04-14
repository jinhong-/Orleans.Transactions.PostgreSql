using System;
using System.Linq;
using System.Reflection;
using Dapper;

namespace Orleans.Transactions.PostgreSql
{
    public class SnakeCaseMap<TType> : SqlMapper.ITypeMap
    {
        private class MemberMap : SqlMapper.IMemberMap
        {
            public string ColumnName { get; set; }
            public Type MemberType { get; set; }
            public PropertyInfo Property { get; set; }
            public FieldInfo Field { get; set; }
            public ParameterInfo Parameter { get; set; }
        }

        public ConstructorInfo FindConstructor(string[] names, Type[] types) =>
            typeof(TType).GetConstructor(new Type[0]);

        public ConstructorInfo FindExplicitConstructor() => null;

        public SqlMapper.IMemberMap GetConstructorParameter(ConstructorInfo constructor, string columnName) =>
            throw new NotSupportedException();

        public virtual PropertyInfo GetPropertyInfo(string columnName)
        {
            var propertyName = string.Concat(columnName.Split(new[] {'_'}, StringSplitOptions.RemoveEmptyEntries)
                .Select(x => char.ToUpperInvariant(x[0]) + x.Substring(1)));
            return typeof(TType).GetProperty(propertyName);
        }

        public virtual SqlMapper.IMemberMap GetMember(string columnName)
        {
            var prop = GetPropertyInfo(columnName);
            return prop != null ? new MemberMap {ColumnName = columnName, Property = prop} : null;
        }
    }
}