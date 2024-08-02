using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace Ceasier.Sql.Driver
{
    public class Pgsql : IDriver
    {
        public DbConnection CreateConnection(string Dsn) => new NpgsqlConnection(Dsn);

        public DbCommand CreateCommand(IDb db, string commandText, CommandType commandType, DbTransaction transaction)
        {
            return new NpgsqlCommand()
            {
                Connection = db.GetConnection() as NpgsqlConnection,
                CommandText = commandText,
                CommandType = commandType,
                Transaction = transaction as NpgsqlTransaction,
            };
        }

        public DbDataReader GetReader(IDb db, string cmd, object parameters, CommandType commandType)
        {
            return db.Run<NpgsqlDataReader>(cmd, parameters, false, commandType);
        }

        public DbParameter CreateParameterFor(DbCommand cmd, string name, object value)
        {
            if (value is NpgsqlParameter param)
            {
                return param;
            }

            return new NpgsqlParameter() { Value = value };
        }

        public string CreateParameterName(QueryBuilder qb, string name) => $"${qb.Params.Count + 1}";

        public bool SupportTableParameter() => false;

        public void TableInsert(IDb db, DataTable dt)
        {
            var conn = db.GetConnection() as NpgsqlConnection;
            var auto = conn.State != ConnectionState.Open;
            var copy = new DTCopy(dt);

            NpgsqlTransaction trans = default;

            if (auto)
            {
                conn.Open();
            }

            try
            {
                trans = conn.BeginTransaction();

                using (var writer = conn.BeginBinaryImport(copy.Cmd))
                {
                    foreach (DataRow row in dt.Rows)
                    {
                        writer.WriteRow(row.ItemArray);
                    }

                    if (dt.Rows.Count > 0)
                    {
                        writer.Complete();
                    }
                }

                trans.Commit();
            }
            catch
            {
                trans?.Rollback();

                throw;
            }
            finally
            {
                if (auto)
                {
                    conn.Close();
                }
            }
        }

        public bool TableExists(IDb db, string tableName)
        {
            return db.Count("pg_tables", new { schemaname = "public", tablename = tableName }, null) > 0;
        }

        public bool TableCreate(IDb db, string tableName, string[] definitions, bool exists)
        {
            return db.TryRun($"CREATE TABLE{(exists ? " IF NOT EXIST" : "")} {tableName} {QueryBuilder.CreateTableDefinitions(definitions)}", null);
        }

        public bool TableDrop(IDb db, string tableName, bool exists)
        {
            return db.TryRun($"DROP TABLE {(exists ? "IF EXISTS" : "")} {tableName}", null);
        }

        public bool TableTruncate(IDb db, string tableName, bool resetIdentity)
        {
            return db.TryRun($"TRUNCATE TABLE {tableName}{(resetIdentity ? " RESTART IDENTITY" : "")}", null);
        }

        public string QuerySelectStart(QueryBuilder qb, string cmd) => cmd;

        public string QuerySelectEnd(QueryBuilder qb, string cmd)
        {
            if (qb.IsLimited(out int limit))
            {
                cmd += $" LIMIT {limit}";
            }

            if (qb.IsStarted(out int offset))
            {
                cmd += $" OFFSET {offset}";
            }

            return cmd;
        }

        public static NpgsqlDbType GetNpgsqlDbType(Type type)
        {
            if (typeof(int) == type)
            {
                return NpgsqlDbType.Integer;
            }

            if (typeof(decimal) == type)
            {
                return NpgsqlDbType.Numeric;
            }

            if (typeof(DateTime) == type || typeof(DateTime?) == type)
            {
                return NpgsqlDbType.Timestamp;
            }

            return NpgsqlDbType.Varchar;
        }

        public bool IsConnectionError(Exception error)
        {
            return error.InnerException.Message.StartsWith("28P01") || error.InnerException.Message.StartsWith("42501");
        }
    }

    public class DTCopy
    {
        public readonly int Count = 0;
        public readonly string Cmd = "";
        public readonly Dictionary<string, NpgsqlDbType> Columns;

        public DTCopy(DataTable dt)
        {
            Columns = new Dictionary<string, NpgsqlDbType>();
            Cmd = $"COPY {dt.TableName} (";

            foreach (DataColumn column in dt.Columns)
            {
                if (Count++ > 0)
                {
                    Cmd += ", ";
                }

                Cmd += column.ColumnName;

                Columns.Add(column.ColumnName, Pgsql.GetNpgsqlDbType(column.DataType));
            }

            Cmd += ") FROM STDIN (FORMAT BINARY)";
        }
    }
}
