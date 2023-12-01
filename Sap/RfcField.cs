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
        public readonly Func<IRfcStructure, object> Getter;
        public readonly Type Type = typeof(string);
        public readonly bool IsNullable = false;

        public RfcField(string name, string column)
        {
            Name = name;
            Column = column ?? name;
        }

        public RfcField(string name) : this(name, name)
        {
        }

        public RfcField(string name, bool nullable): this(name)
        {
            IsNullable = nullable;
        }

        public RfcField(string name, Type type) : this(name)
        {
            Type = type;
        }

        public RfcField(string name, Func<IRfcStructure, object> getter) : this(name)
        {
            Getter = getter;
        }

        public RfcField(string name, string column, object value): this(name, column, value, null)
        {
        }

        public RfcField(string name, string column, Type type) : this(name, column, null, type)
        {
        }

        public RfcField(string name, string column, object value, Type type): this(name, column)
        {
            Value = value;
            Type = type;

            if (null == type && null != value)
            {
                Type = value.GetType();
            }
        }

        public object GetValue(IRfcStructure row)
        {
            object value;

            if (IsNullable)
            {
                value = null;
            }
            else if (null != Value)
            {
                value = Value;
            }
            else if (null != Getter)
            {
                value = Getter.Invoke(row);
            }
            else if (typeof(int) == Type)
            {
                value = row.GetInt(Name);
            }
            else if (typeof(decimal) == Type)
            {
                value = row.GetDecimal(Name);
            }
            else if (typeof(DateTime) == Type || typeof(DateTime?) == Type)
            {
                value = ToSafeDate(row.GetString(Name));
            }
            else
            {
                value = row.GetString(Name);
            }

            if (null == value || (value is string s && "" == s))
            {
                return null;
            }

            return value;
        }

        public static DateTime? ToSafeDate(string str)
        {
            if (string.IsNullOrEmpty(str) || str.Count() < 10)
            {
                return null;
            }

            if (str.Contains('-') && DateTime.TryParse(str, out DateTime ndate))
            {
                return ndate;
            }

            if (str.Contains('.') && DateTime.TryParse($"{str.Substring(6, 4)}-{str.Substring(3, 2)}-{str.Substring(0, 2)}", out DateTime sdate))
            {
                return sdate;
            }

            return null;
        }
    }
}
