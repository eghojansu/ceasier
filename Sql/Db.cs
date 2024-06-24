using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using Ceasier.Utils;

namespace Ceasier.Sql
{
    public class Db : IDb
    {
        private static readonly Type ReaderType = typeof(DbDataReader);

        private readonly string Dsn;

        private readonly IDriver Driver;

        private DbConnection _connection;

        public Db(IDriver driver, string connectionString)
        {
            Driver = driver;
            Dsn = connectionString;
        }

        public DbConnection Connection => GetConnection();

        public DbConnection GetConnection()
        {
            if (null != _connection)
            {
                return _connection;
            }

            if (string.IsNullOrEmpty(Dsn))
            {
                throw new ArgumentNullException("Connection is not defined properly");
            }

            _connection = Driver.CreateConnection(Dsn);

            return _connection;
        }

        public T Run<T>(DbCommand cmd, object parameters, bool scalar)
        {
            var auto = cmd.Connection.State != ConnectionState.Open;
            var expectedType = typeof(T);
            dynamic result;

            try
            {
                if (null != parameters)
                {
                    AddParameters(cmd, parameters);
                }

                if (auto)
                {
                    cmd.Connection.Open();
                }

                if (IsReading<T>())
                {
                    auto = false;
                    result = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                }
                else if (scalar)
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

                return (T) Convert.ChangeType(result, expectedType);
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

        public T Run<T>(string cmd, object parameters, bool scalar, CommandType commandType)
        {
            return Run<T>(Driver.CreateCommand(this, cmd, commandType, null), parameters, scalar);
        }

        public T Run<T>(QueryBuilder qb)
        {
            return Run<T>(qb.Sql, qb.Params, qb.Scalar, CommandType.Text);
        }

        public bool TryRun(string cmd, object parameters)
        {
            try
            {
                Run<int>(cmd, parameters, false, CommandType.Text);
            }
            catch (Exception e)
            {
                if (Driver.IsConnectionError(e))
                {
                    throw e;
                }

                return false;
            }

            return true;
        }

        public bool TryRun(string cmd) => TryRun(cmd, null);

        public DbDataReader Run(string cmd, object parameters, CommandType commandType)
        {
            return Driver.GetReader(this, cmd, parameters, commandType);
        }

        public DbDataReader Run(QueryBuilder qb)
        {
            return Run(qb.Sql, qb.Params, CommandType.Text);
        }

        public List<Dictionary<string, object>> Result(string cmd, object parameters, CommandType commandType)
        {
            return ReadRows(Run(cmd, parameters, commandType));
        }

        public List<Dictionary<string, object>> Result(string cmd, CommandType commandType)
        {
            return Result(cmd, null, commandType);
        }

        public List<Dictionary<string, object>> Result(QueryBuilder qb)
        {
            return Result(qb.Sql, qb.Params, CommandType.Text);
        }

        public Dictionary<string, object> ResultFirst(string cmd, object parameters, CommandType commandType)
        {
            return Result(cmd, parameters, commandType).FirstOrDefault();
        }

        public Dictionary<string, object> ResultFirst(string cmd, CommandType commandType)
        {
            return ResultFirst(cmd, null, commandType);
        }

        public Dictionary<string, object> ResultFirst(QueryBuilder qb)
        {
            return ResultFirst(qb.Sql, qb.Params, CommandType.Text);
        }

        public T Query<T>(string cmd, object parameters, bool scalar)
        {
            return Run<T>(cmd, parameters, scalar, CommandType.Text);
        }

        public DbDataReader Query(string cmd, object parameters)
        {
            return Run(cmd, parameters, CommandType.Text);
        }

        public DbDataReader Query(string cmd)
        {
            return Query(cmd, null);
        }

        public List<Dictionary<string, object>> QueryResult(string cmd, object parameters)
        {
            return Result(cmd, parameters, CommandType.Text);
        }

        public List<Dictionary<string, object>> QueryResult(string cmd)
        {
            return QueryResult(cmd, null);
        }

        public Dictionary<string, object> QueryFirst(string cmd, object parameters)
        {
            return ResultFirst(cmd, parameters, CommandType.Text);
        }

        public Dictionary<string, object> QueryFirst(string cmd)
        {
            return QueryFirst(cmd, null);
        }

        public T Sp<T>(string name, object parameters, bool scalar)
        {
            if (!scalar && !IsReading<T>())
            {
                var cmd = Driver.CreateCommand(this, name, CommandType.StoredProcedure, null);

                AddParameters(cmd, parameters);
                AddParameter(cmd, "_", null);

                return Run<T>(cmd, null, scalar);
            }

            return Run<T>(name, parameters, scalar, CommandType.StoredProcedure);
        }

        public T Sp<T>(string name, object parameters)
        {
            return Sp<T>(name, parameters, false);
        }

        public T Sp<T>(string name, bool scalar)
        {
            return Sp<T>(name, null, scalar);
        }

        public T Sp<T>(string name)
        {
            return Sp<T>(name, false);
        }

        public DbDataReader Sp(string name, object parameters)
        {
            return Run(name, parameters, CommandType.StoredProcedure);
        }

        public DbDataReader Sp(string name)
        {
            return Sp(name, null);
        }

        public List<Dictionary<string, object>> SpResult(string name, object parameters)
        {
            return Result(name, parameters, CommandType.StoredProcedure);
        }

        public List<Dictionary<string, object>> SpResult(string name)
        {
            return SpResult(name, null);
        }

        public Dictionary<string, object> SpFirst(string name, object parameters)
        {
            return ResultFirst(name, parameters, CommandType.StoredProcedure);
        }

        public Dictionary<string, object> SpFirst(string name)
        {
            return SpFirst(name, null);
        }

        public T Fn<T>(string name, object parameters, bool scalar)
        {
            return Run<T>(Qb().CallFunction(name, parameters, scalar));
        }

        public T Fn<T>(string name, object parameters)
        {
            return Fn<T>(name, parameters, true);
        }

        public T Fn<T>(string name, bool scalar)
        {
            return Fn<T>(name, null, scalar);
        }

        public T Fn<T>(string name)
        {
            return Fn<T>(name, true);
        }

        public DbDataReader Fn(string name, object parameters)
        {
            return Run(Qb().CallFunction(name, parameters, false));
        }

        public DbDataReader Fn(string name)
        {
            return Fn(name, null);
        }

        public List<Dictionary<string, object>> FnResult(string name, object parameters)
        {
            return Result(Qb().CallFunction(name, parameters, false));
        }

        public List<Dictionary<string, object>> FnResult(string name)
        {
            return FnResult(name, null);
        }

        public Dictionary<string, object> FnFirst(string name, object parameters)
        {
            return ResultFirst(Qb().CallFunction(name, parameters, false));
        }

        public Dictionary<string, object> FnFirst(string name)
        {
            return FnFirst(name, null);
        }

        public DbDataReader Read(string table, object filters, Dictionary<string, object> options)
        {
            return Run(Qb().Find(table, filters, options));
        }

        public DbDataReader Read(string table, object filters)
        {
            return Read(table, filters, null);
        }

        public DbDataReader Read(string table)
        {
            return Read(table, null);
        }

        public List<Dictionary<string, object>> Find(string table, object filters, Dictionary<string, object> options)
        {
            return Result(Qb().Find(table, filters, options));
        }

        public List<Dictionary<string, object>> Find(string table, object filters)
        {
            return Find(table, filters, null);
        }

        public List<Dictionary<string, object>> Find(string table)
        {
            return Find(table, null);
        }

        public Dictionary<string, object> First(string table, object filters, Dictionary<string, object> options)
        {
            return ResultFirst(Qb().Find(table, filters, options));
        }

        public Dictionary<string, object> First(string table, object filters)
        {
            return First(table, filters, null);
        }

        public Dictionary<string, object> First(string table)
        {
            return First(table, null);
        }

        public int Count(string table, object filters, Dictionary<string, object> options)
        {
            return Convert.ToInt32(ResultFirst(Qb().Find(table, filters, options).Select("count(*) AS c"))?["c"] ?? 0);
        }

        public int Count(string table, object filters)
        {
            return Count(table, filters, null);
        }

        public int Count(string table)
        {
            return Count(table, null);
        }

        public int Insert(string table, object data) => Run<int>(Qb().Insert(table, data));

        public void Insert(DataTable dt) => Driver.TableInsert(this, dt);

        public void Insert(string spName, DataTable dt)
        {
            if (!Driver.SupportTableParameter())
            {
                throw new InvalidOperationException($"Driver not supporting table parameter: {Driver.GetType().Name}");
            }

            Sp<int>(spName, new Dictionary<string, object>() { { dt.TableName, dt} });
        }

        public int Update(string table, object data, object filters) => Run<int>(Qb().Update(table, data, filters));

        public int Delete(string table, object filters) => Run<int>(Qb().Delete(table, filters));

        public bool Exists(string tableName) => Driver.TableExists(this, tableName);

        public bool Create(string tableName, string[] definitions, bool exists) => Driver.TableCreate(this, tableName, definitions, exists);

        public bool Create(string tableName, string[] definitions) => Create(tableName, definitions, false);

        public bool Drop(string tableName, bool exists) => Driver.TableDrop(this, tableName, exists);

        public bool Drop(string tableName) => Drop(tableName, false);

        public bool Truncate(string tableName, bool resetIdentity) => Driver.TableTruncate(this, tableName, resetIdentity);

        public bool Truncate(string tableName) => Truncate(tableName, false);

        public QueryBuilder Qb() => new QueryBuilder(Driver);

        public List<Dictionary<string, object>> ReadRows(DbDataReader reader, bool close)
        {
            var rows = new List<Dictionary<string, object>>();

            while (reader.Read())
            {
                rows.Add(ReadRow(reader));
            }

            if (close)
            {
                reader.Close();
            }

            return rows;
        }

        public List<Dictionary<string, object>> ReadRows(DbDataReader reader) => ReadRows(reader, true);

        public Dictionary<string, object> ReadRow(DbDataReader reader)
        {
            var item = new Dictionary<string, object>();

            for (int i = 0; i < reader.FieldCount; i++)
            {
                item.Add(reader.GetName(i), reader.GetValue(i));
            }

            return item;
        }

        private void AddParameter(DbCommand cmd, string name, object value)
        {
            cmd.Parameters.Add(CreateParameter(cmd, name, value));
        }

        private DbCommand AddParameters(DbCommand cmd, object parameters)
        {
            if (parameters is object[] positionals)
            {
                foreach (var value in positionals)
                {
                    AddParameter(cmd, null, value);
                }
            }
            else
            {
                Common.ObjectMap(parameters, (string name, object value) => AddParameter(cmd, name, value));
            }

            return cmd;
        }

        private DbParameter CreateParameter(DbCommand cmd, string name, object value)
        {
            if (null != name && name.Equals("_"))
            {
                var returnName = value is string n ? n : "returnValue";
                var param = Driver.CreateParameterFor(cmd, returnName, null);

                param.Direction = ParameterDirection.ReturnValue;
                param.DbType = DbType.Int32;

                return param;
            }

            var v = value is string s && s == "" ? string.Empty : value;

            return Driver.CreateParameterFor(cmd, name, v ?? DBNull.Value);
        }

        private static bool TryGetReturnParameter(DbCommand cmd, out int value)
        {
            value = 0;

            foreach (DbParameter item in cmd.Parameters)
            {
                if (item.Direction == ParameterDirection.ReturnValue)
                {
                    value = (int)item.Value;

                    return true;
                }
            }

            return false;
        }

        private static bool IsReading<T>() => ReaderType.IsAssignableFrom(typeof(T));
    }
}
