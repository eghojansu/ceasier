using SAP.Middleware.Connector;
using System;
using System.Linq;

namespace Ceasier.Sap
{
    public class RfcField
    {
        public readonly string Name;
        public readonly string Column;
        public readonly object Value;
        public readonly Type Type = typeof(string);
        public readonly bool IsNullable = false;

        public RfcField(string name, object column)
        {
            Name = name;

            if (column is string ucolumn)
            {
                Column = ucolumn;
            }
            else
            {
                Column = name;
            }

            if (column is bool nullable)
            {
                IsNullable = nullable;
            }
        }

        public RfcField(string name): this(name, name)
        {
        }

        public RfcField(string name, object column, object value): this(name, column)
        {
            if (value is Type type)
            {
                Type = type;
            }
            else if (null != value)
            {
                Value = value;
                Type = value.GetType();
            }
        }

        public object GetValue(IRfcStructure row)
        {
            if (IsNullable)
            {
                return null;
            }

            if (null != Value)
            {
                if (Value is Func<IRfcStructure, object> val)
                {
                    return val.Invoke(row);
                }

                return Value;
            }

            if (typeof(int) == Type)
            {
                return row.GetInt(Name);
            }

            if (typeof(decimal) == Type)
            {
                return row.GetDecimal(Name);
            }

            if (typeof(DateTime) == Type || typeof(DateTime?) == Type)
            {
                return ToSafeDate(row.GetString(Name));
            }

            return row.GetString(Name);
        }

        public static DateTime? ToSafeDate(string str)
        {
            if (string.IsNullOrEmpty(str) || str.Count() < 10)
            {
                return default;
            }

            if (str.Contains('-') && DateTime.TryParse(str, out DateTime ndate))
            {
                return ndate;
            }

            if (str.Contains('.') && DateTime.TryParse($"{str.Substring(6, 4)}-{str.Substring(3, 2)}-{str.Substring(0, 2)}", out DateTime sdate))
            {
                return sdate;
            }

            return default;
        }
    }
}
