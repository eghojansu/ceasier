using Ceasier.Utils;
using System;
using System.Collections.Generic;

namespace Ceasier.Sql
{
    public class QueryBuilder
    {
        private enum Query
        {
            SELECT,
            INSERT,
            UPDATE,
            DELETE,
            FUNCTION,
        }

        private readonly Dictionary<string, object> _parts = new Dictionary<string, object>();
        private readonly Dictionary<string, object> _params = new Dictionary<string, object>();
        private readonly List<string> _filters = new List<string>();
        private readonly IDriver Driver;
        private string _sql;
        private bool? _scalar;

        public QueryBuilder(IDriver driver)
        {
            Driver = driver;
        }

        public string Sql
        {
            get
            {
                if (_sql == null)
                {
                    WithPart("action", out Query action);

                    if (Query.INSERT == action)
                    {
                        _sql = BuildInsert();
                    }
                    else if (Query.UPDATE == action)
                    {
                        _sql = BuildUpdate();
                    }
                    else if (Query.DELETE == action)
                    {
                        _sql = BuildDelete();
                    }
                    else if (Query.FUNCTION == action)
                    {
                        _sql = BuildCallFunction();
                    }
                    else
                    {
                        _sql = BuildSelect();
                    }
                }

                return _sql;
            }
        }

        public bool Scalar => _scalar ?? (!WithPart("action", out Query action) || action == Query.SELECT);

        public Dictionary<string, object> Params => _params;

        public QueryBuilder Insert(string table, object data)
        {
            var columns = new List<string>();

            Common.ObjectMap(data, (string name, object value) => {
                columns.Add(name);
                _params.Add(Driver.CreateParameterName(this, name), value);
            });

            return From(table).AddPart("action", Query.INSERT).AddPart("line", "(" + string.Join(", ", columns) + ") VALUES (" + string.Join(", ", _params.Keys) + ")");
        }

        public QueryBuilder Update(string table, object data, object filters)
        {
            var line = "";

            Common.ObjectMap(data, (string key, object value) =>
            {
                var name = Driver.CreateParameterName(this, key);

                _params.Add(name, value);

                line += $"{("" == line ? "" : ", ")}{key} = {name}";
            });

            return From(table).Where(filters).AddPart("action", Query.UPDATE).AddPart("line", line);
        }

        public QueryBuilder Delete(string table, object filters) => From(table).Where(filters).AddPart("action", Query.DELETE);

        public QueryBuilder Select() => AddPart("columns", "*");

        public QueryBuilder Select(params string[] columns) => AddPart("columns", string.Join(", ", columns));

        public QueryBuilder Select(string columns) => AddPart("columns", columns);

        public QueryBuilder From(string table) => From(table, null);

        public QueryBuilder From(string table, string alias) => AddPart("table", table).As(alias);

        public QueryBuilder As(string alias) => AddPart("alias", alias);

        public QueryBuilder Where(object filters)
        {
            if (filters is bool f)
            {
                return ScalarQuery(f);
            }

            Common.ObjectMap(filters, (string name, object value) => AndWhere(name, value));

            return this;
        }

        public QueryBuilder Where(string column, object value) => Where(column, value, "=", "AND");

        public QueryBuilder Where(string column, object value, string opr, string conj) => Where(column, value, opr, conj, true);

        public QueryBuilder Where(string column, object value, string opr, string conj, bool reset)
        {
            if (reset)
            {
                _filters.Clear();
            }

            var name = Driver.CreateParameterName(this, column);

            return AddFilter(name, value, $"{conj} {column} {opr} {name}");
        }

        public QueryBuilder AndWhere(string column, object value) => AndWhere(column, value, "=");

        public QueryBuilder AndWhere(string column, object value, string opr) => Where(column, value, opr, "AND", false);

        public QueryBuilder OrWhere(string column, object value) => OrWhere(column, value, "=");

        public QueryBuilder OrWhere(string column, object value, string opr) => Where(column, value, opr, "OR", false);

        public QueryBuilder OrderBy(string order) => AddPart("order", order);

        public QueryBuilder GroupBy(string group) => AddPart("group", group);

        public QueryBuilder Offset(int offset) => AddPart("offset", offset);

        public QueryBuilder Limit(int limit) => Limit(limit, 0);

        public QueryBuilder Limit(int limit, int offset)
        {
            if (offset > 0)
            {
                Offset(offset);
            }

            return AddPart("limit", limit);
        }

        public QueryBuilder Find(string table, object filters, Dictionary<string, object> options) => From(table).Where(filters).AddParts(options);

        public QueryBuilder First(string table, object filters, Dictionary<string, object> options) => Find(table, filters, options).Limit(1);

        public QueryBuilder ScalarQuery(bool scalar)
        {
            _scalar = scalar;

            return this;
        }

        public QueryBuilder CallFunction(string fnName, object parameters, bool scalar)
        {
            if (null != parameters)
            {
                Common.ObjectMap(parameters, (string key, object value) =>
                {
                    var name = Driver.CreateParameterName(this, key);

                    AddFilter(name, value, name);
                });
            }

            return From(fnName).ScalarQuery(scalar).AddPart("action", Query.FUNCTION);
        }

        public QueryBuilder AddFilter(string name, object value, string expr)
        {
            _filters.Add(expr);
            _params.Add(name, value);

            return this;
        }

        public QueryBuilder AddPart(string part, object value)
        {
            if (_parts.ContainsKey(part))
            {
                _parts.Remove(part);
            }

            _parts.Add(part, value);

            return this;
        }

        public QueryBuilder AddParts(Dictionary<string, object> options)
        {
            if (null != options)
            {
                foreach (var key in options.Keys)
                {
                    AddPart(key, options[key]);
                }
            }

            return this;
        }

        public bool WithPart<T>(string part, out T output)
        {
            output = default;

            if (_parts.TryGetValue(part, out object value))
            {
                output = (T)Convert.ChangeType(value, typeof(T));
            }

            return null != output && (!(output is string s) || !string.IsNullOrEmpty(s));
        }

        public bool IsLimited(out int limit)
        {
            var limited = WithPart("limit", out int val) && val > 0;

            limit = val;

            return limited;
        }

        public bool IsStarted()
        {
            return IsStarted(out int _);
        }

        public bool IsStarted(out int offset)
        {
            var starting = WithPart("offset", out int val) && val > 0;

            offset = val;

            return starting;
        }

        private string BuildInsert() => $"INSERT INTO {_parts["table"]} {_parts["line"]}";

        private string BuildUpdate() => $"UPDATE {_parts["table"]} SET {_parts["line"]}{BuildFilters()}";

        private string BuildDelete() => $"DELETE FROM {_parts["table"]}{BuildFilters()}";

        private string BuildCallFunction() => $"{BuildSelect(false)}({BuildFilters("", ", ")})";

        private string BuildSelect() => BuildSelect(true);

        private string BuildSelect(bool filters)
        {
            var sql = Driver.QuerySelectStart(this, "SELECT");

            if (_parts.ContainsKey("columns"))
            {
                sql += $" {_parts["columns"]}";
            }
            else
            {
                sql += " *";
            }

            sql += $" FROM {_parts["table"]}";

            if (WithPart("alias", out string alias))
            {
                sql += $" AS {alias}";
            }

            if (filters)
            {
                sql += BuildFilters();
            }

            if (WithPart("group", out string group))
            {
                sql += $" GROUP BY {group}";
            }

            if (WithPart("order", out string order))
            {
                sql += $" ORDER BY {order}";
            }

            return Driver.QuerySelectEnd(this, sql);
        }

        private string BuildFilters() => BuildFilters(" WHERE ", " ");

        private string BuildFilters(string prefix, string separator)
        {
            var add = "";
            var filters = "";

            if (_filters.Count > 0)
            {
                var criteria = string.Join(separator, _filters);
                var start = criteria.IndexOf(" ");

                if (start > 0)
                {
                    criteria = criteria.Substring(start + 1).Trim();
                }

                if (!string.IsNullOrEmpty(criteria))
                {
                    add = prefix;
                    filters = criteria;
                }
            }

            return $"{add}{filters}";
        }

        public static string CreateTableDefinitions(string[] definitions)
        {
            var cmd = "";
            var last = definitions.Length - 1;
            var option = false;

            for (var i = 0; i <= last; i++)
            {
                var row = definitions[i];

                if ("--" == row)
                {
                    option = true;

                    continue;
                }

                if (i == 0)
                {
                    cmd += "(";
                }

                cmd += row;

                if (option)
                {
                    cmd += i == last ? "" : " ";
                }
                else
                {
                    cmd += i == last || "--" == definitions[i + 1] ? ") " : ", ";
                }
            }

            return cmd.Trim();
        }
    }
}
