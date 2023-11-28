using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using Ceasier.Utils;

namespace Ceasier.Sql
{
    public abstract class Db<R> where R: DbDataReader
    {
        private static readonly Type ReaderType = typeof(DbDataReader);

        protected string Dsn;

        protected abstract DbConnection CreateConnection(string Dsn);

        protected abstract DbParameter CreateParameterFor(DbCommand cmd, string name, object value);

        public abstract QueryBuilder Qb();

        public abstract DbCommand CreateCommand(string commandText, CommandType commandType, DbTransaction transaction);

        public abstract void Insert(DataTable dt);

        public abstract void Insert(string spName, DataTable dt);

        public abstract bool Exists(string tableName);

        public abstract bool Create(string tableName, string[] definitions, bool exists);

        public abstract bool Drop(string tableName, bool exists);

        public abstract bool Truncate(string tableName, bool resetIdentity);

        public Db(string connectionString) => Dsn = connectionString;

        public DbConnection Connection
        {
            get
            {
                if (string.IsNullOrEmpty(Dsn))
                {
                    throw new ArgumentNullException("Connection is not defined properly");
                }

                return CreateConnection(Dsn);
            }
        }

        public bool Create(string tableName, string[] definitions) => Create(tableName, definitions, false);

        public bool Drop(string tableName) => Drop(tableName, false);

        public bool Truncate(string tableName) => Truncate(tableName, false);

        public T Run<T>(QueryBuilder qb) => Run<T>(qb.Sql, qb.Params, qb.Scalar, CommandType.Text);

        public T Run<T>(string commandText, object parameters, bool scalar, CommandType commandType) => Run<T>(commandText, parameters, scalar, commandType, null);

        public T Run<T>(string commandText, object parameters, bool scalar, CommandType commandType, DbTransaction transaction)
        {
            return Run<T>(CreateCommand(commandText, commandType, transaction), parameters, scalar);
        }

        public T Run<T>(DbCommand cmd, object parameters, bool scalar)
        {
            var auto = cmd.Connection.State != ConnectionState.Open;
            var expectedType = typeof(T);
            var readerType = typeof(DbDataReader);
            var scalar_ = parameters is bool p ? p : scalar;
            var parameters_ = parameters is bool ? null : parameters;
            dynamic result;

            try
            {
                if (null != parameters_)
                {
                    AddParameters(cmd, parameters_);
                }

                if (auto)
                {
                    cmd.Connection.Open();
                }

                if (expectedType == readerType || expectedType.IsSubclassOf(readerType))
                {
                    auto = false;
                    result = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                }
                else if (scalar_)
                {
                    result = cmd.ExecuteScalar();
                }
                else
                {
                    result = cmd.ExecuteNonQuery();

                    if (TryGetReturnParameter(cmd, out int ret))
                    {
                        result = ret;
                    }
                }

                return (T)Convert.ChangeType(result, expectedType);
            }
            catch (Exception ex)
            {
                throw new Exception("Error execution of " + cmd.CommandText, ex);
            }
            finally
            {
                if (auto)
                {
                    cmd.Connection.Close();
                }
            }
        }

        public List<Dictionary<string, object>> SPResult(string procedureName) => SPResult(procedureName, null);

        public List<Dictionary<string, object>> SPResult(string procedureName, object parameters) => BuildRows(SPRun(procedureName, parameters));

        public Dictionary<string, object> SPFirst(string procedureName) => SPFirst(procedureName, null);

        public Dictionary<string, object> SPFirst(string procedureName, object parameters) => SPResult(procedureName, parameters)?.ElementAtOrDefault(0);

        public DbDataReader SPRun(string procedureName, object parameters) => SPRun<R>(procedureName, parameters);

        public T SPRun<T>(string procedureName) => SPRun<T>(procedureName, null);

        public T SPRun<T>(string procedureName, object parameters) => SPRun<T>(procedureName, parameters, false);

        public T SPRun<T>(string procedureName, object parameters, bool scalar)
        {
            var values = parameters;

            if (!scalar && !IsReading<T>())
            {
                var args = Common.ObjectValues(parameters);
                var param = CreateParameter(null, "_", null);

                args.Add(new KeyValuePair<string, object>(param.ParameterName, param));

                values = args;
            }

            return Run<T>(procedureName, values, scalar, CommandType.StoredProcedure);
        }

        public List<Dictionary<string, object>> FnResult(string functionName) => FnResult(functionName, null);

        public List<Dictionary<string, object>> FnResult(string functionName, object parameters) => BuildRows(FnRun(functionName, parameters));

        public Dictionary<string, object> FnFirst(string functionName) => FnFirst(functionName, null);

        public Dictionary<string, object> FnFirst(string functionName, object parameters) => FnResult(functionName, parameters)?.ElementAtOrDefault(0);

        public DbDataReader FnRun(string functionName, object parameters) => FnRun<R>(functionName, parameters);

        public T FnRun<T>(string functionName) => FnRun<T>(functionName, null);

        public T FnRun<T>(string functionName, object parameters) => FnRun<T>(functionName, parameters, true);

        public T FnRun<T>(string functionName, object parameters, bool scalar) => Run<T>(Qb().CallFunction(functionName, parameters, scalar));

        public bool TryQuery(string sql) => TryQuery(sql, null);

        public bool TryQuery(string sql, object parameters)
        {
            try
            {
                Query<int>(sql, parameters, false);

                return true;
            } catch
            {
                return false;
            }
        }

        public List<Dictionary<string, object>> QueryResult(QueryBuilder qb) => QueryResult(qb.Sql, qb.Params);

        public List<Dictionary<string, object>> QueryResult(string sql) => QueryResult(sql, null);

        public List<Dictionary<string, object>> QueryResult(string sql, object parameters) => BuildRows(Query(sql, parameters));

        public Dictionary<string, object> QueryFirst(QueryBuilder qb) => QueryFirst(qb.Sql, qb.Params);

        public Dictionary<string, object> QueryFirst(string sql) => QueryFirst(sql, null);

        public Dictionary<string, object> QueryFirst(string sql, object parameters) => QueryResult(sql, parameters)?.ElementAtOrDefault(0);

        public DbDataReader Query(QueryBuilder qb) => Query(qb.Sql, qb.Params);

        public DbDataReader Query(string sql) => Query(sql, null);

        public DbDataReader Query(string sql, object parameters) => Query<R>(sql, parameters, false);

        public T Query<T>(string sql) => Query<T>(sql, false);

        public T Query<T>(string sql, object parameters) => Query<T>(sql, parameters, false);

        public T Query<T>(string sql, object parameters, bool scalar) => Run<T>(sql, parameters, scalar, CommandType.Text);

        public T Query<T>(QueryBuilder qb) => Query<T>(qb.Sql, qb.Params, qb.Scalar);

        public DbDataReader Fetch(string table) => Fetch(table, null);

        public DbDataReader Fetch(string table, object filters) => Fetch(table, filters, null);

        public DbDataReader Fetch(string table, object filters, Dictionary<string, object> options) => Query(Qb().Find(table, filters, options));

        public List<Dictionary<string, object>> Find(string table) => Find(table, null);

        public List<Dictionary<string, object>> Find(string table, object filters) => Find(table, filters, null);

        public List<Dictionary<string, object>> Find(string table, object filters, Dictionary<string, object> options) => QueryResult(Qb().Find(table, filters, options));

        public Dictionary<string, object> First(string table) => First(table, null);

        public Dictionary<string, object> First(string table, object filters) => First(table, filters, null);

        public Dictionary<string, object> First(string table, object filters, Dictionary<string, object> options) => QueryFirst(Qb().First(table, filters, options));

        public int Count(string table) => Count(table, null);

        public int Count(string table, object filters) => Count(table, filters, null);

        public int Count(string table, object filters, Dictionary<string, object> options)
        {
            return Convert.ToInt32(QueryFirst(Qb().Find(table, filters, options).Select("count(*) AS c"))?["c"] ?? 0);
        }

        public int Insert(string table, object data) => Query<int>(Qb().Insert(table, data));

        public int Update(string table, object data, object filters) => Query<int>(Qb().Update(table, data, filters));

        public int Delete(string table, object filters) => Query<int>(Qb().Delete(table, filters));

        public List<Dictionary<string, object>> BuildRows(DbDataReader row) => BuildRows(row, true);

        public List<Dictionary<string, object>> BuildRows(DbDataReader row, bool close)
        {
            var rows = new List<Dictionary<string, object>>();

            while (row.Read())
            {
                rows.Add(BuildRow(row));
            }

            if (close)
            {
                row.Close();
            }

            return rows;
        }

        public Dictionary<string, object> BuildRow(DbDataReader row)
        {
            var item = new Dictionary<string, object>();

            for (int i = 0; i < row.FieldCount; i++)
            {
                item.Add(row.GetName(i), row.GetValue(i));
            }

            return item;
        }

        private DbParameter CreateParameter(DbCommand cmd, string name, object value)
        {
            if (null != name && name.Equals("_"))
            {
                var returnName = value is string n ? n : "returnValue";
                var param = CreateParameterFor(cmd, returnName, null);

                param.Direction = ParameterDirection.ReturnValue;
                param.DbType = DbType.Int32;

                return param;
            }

            return CreateParameterFor(cmd, name, value ?? DBNull.Value);
        }

        private DbCommand AddParameters(DbCommand cmd, object parameters)
        {
            if (parameters is object[] positionals)
            {
                foreach (var value in positionals)
                {
                    cmd.Parameters.Add(CreateParameter(cmd, null, value));
                }
            } else
            {
                Common.ObjectValues(parameters).ForEach(set =>
                {
                    cmd.Parameters.Add(CreateParameter(cmd, set.Key, set.Value));
                });
            }

            return cmd;
        }

        private bool TryGetReturnParameter(DbCommand cmd, out int value)
        {
            value = 0;

            foreach (DbParameter item in cmd.Parameters)
            {
                if (item.Direction == ParameterDirection.ReturnValue)
                {
                    value = (int) item.Value;

                    return true;
                }
            }

            return false;
        }

        private bool IsReading<T>() => ReaderType.IsAssignableFrom(typeof(T));
    }
}
