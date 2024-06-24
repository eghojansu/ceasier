using Ceasier.Utils;
using SAP.Middleware.Connector;
using System;
using System.Collections.Generic;
using System.Linq;
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;
using static OfficeOpenXml.ExcelErrorValue;

namespace Ceasier.Sap
{
    public class RfcField
    {
        public readonly string Name;
        public readonly string Column;
        public readonly object Value;
        public readonly bool IsNullable;
        public readonly Type ValueType;
        public readonly Func<IRfcStructure, object> RfcGetter;
        public readonly Func<Dictionary<string, object>, object> DicGetter;

        public RfcField(string name, string column, bool nullable, object value, Type type)
        {
            Name = name;
            Column = column ?? name;
            Value = value;
            IsNullable = nullable;
            ValueType = type ?? value?.GetType() ?? typeof(string);
        }

        public RfcField(string name, string column, object value, Type type) : this(name, column, false, value, type)
        {
        }

        public RfcField(string name, string column, object value): this(name, column, value, null)
        {
        }

        public RfcField(string name, string column, Type type) : this(name, column, null, type)
        {
        }

        public RfcField(string name, string column) : this(name, column, null)
        {
        }
        public RfcField(string name, bool nullable) : this(name, null, nullable, null, null)
        {
        }

        public RfcField(string name, Type type) : this(name, null, true, null, type)
        {
        }

        public RfcField(string name) : this(name, name)
        {
        }

        public RfcField(string name, Func<IRfcStructure, object> getter, Type type) : this(name, null, false, null, type)
        {
            RfcGetter = getter;
        }

        public RfcField(string name, Func<IRfcStructure, object> getter) : this(name, getter, null)
        {
        }

        public RfcField(string name, Func<Dictionary<string, object>, object> getter, Type type) : this(name, null, false, null, type)
        {
            DicGetter = getter;
        }

        public RfcField(string name, Func<Dictionary<string, object>, object> getter) : this(name, getter, null)
        {
        }

        public object GetValue(IRfcStructure row)
        {
            object value;

            if (IsNullable)
            {
                value = null;
            }
            else if (null != RfcGetter)
            {
                value = RfcGetter.Invoke(row);
            }
            else if (null != Value)
            {
                value = Value;
            } else if (Common.IntType == ValueType)
            {
                value = row.GetInt(Name);
            }
            else if (Common.DecimalType == ValueType)
            {
                value = row.GetDecimal(Name);
            }
            else if (Common.FloatType == ValueType || Common.DoubleType == ValueType)
            {
                value = row.GetDouble(Name);
            }
            else if (Common.DateTimeType == ValueType)
            {
                value = ToSafeDate(row.GetString(Name)) ?? default;
            } else
            {
                value = row.GetString(Name);
            }

            return value;
        }

        public object GetValue(Dictionary<string, object> row)
        {
            object value;

            if (null != DicGetter)
            {
                value = DicGetter.Invoke(row);
            }
            else if (null != Value)
            {
                value = Value;
            }
            else if (!row.TryGetValue(Column, out object val) || val is DBNull)
            {
                value = null;
            }
            else if (Common.IntType == ValueType)
            {
                value = Convert.ToInt32(val);
            }
            else if (Common.DecimalType == ValueType)
            {
                value = Convert.ToDecimal(val);
            }
            else if (Common.FloatType == ValueType || Common.DoubleType == ValueType)
            {
                value = Convert.ToDouble(val);
            }
            else if (Common.DateTimeType == ValueType)
            {
                value = ToSafeDate(val.ToString()) ?? default;
            }
            else
            {
                value = val;
            }

            return value;
        }

        public static DateTime? ToSafeDate(string str)
        {
            if (string.IsNullOrEmpty(str) || str.Count() < 10)
            {
                return null;
            }

            var txt = str.Contains('.') ? $"{str.Substring(6, 4)}-{str.Substring(3, 2)}-{str.Substring(0, 2)}" : str;

            try
            {
                return DateTime.Parse(txt);
            }
            catch
            {
                try
                {
                    return Convert.ToDateTime(txt);
                }
                catch
                {
                    return null;
                }
            }
        }
    }
}
