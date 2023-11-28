using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace Ceasier.Sql
{
    public class Pgsql: Db<NpgsqlDataReader>
    {
        public Pgsql(string connectionString) : base(connectionString)
        {
        }

        protected override DbConnection CreateConnection(string Dsn) => new NpgsqlConnection(Dsn);

        protected override DbParameter CreateParameterFor(DbCommand cmd, string name, object value)
        {
            if (value is NpgsqlParameter param)
            {
                return param;
            }

            return new NpgsqlParameter() { Value = value };
        }

        public override QueryBuilder Qb() => new PgQueryBuilder();

        public override DbCommand CreateCommand(string commandText, CommandType commandType, DbTransaction transaction) => new NpgsqlCommand()
        {
            Connection = Connection as NpgsqlConnection,
            CommandText = commandText,
            CommandType = commandType,
            Transaction = transaction as NpgsqlTransaction,
        };

        public override void Insert(DataTable dt)
        {
            var conn = Connection as NpgsqlConnection;
            var copy = new DTCopy(dt);

            NpgsqlTransaction trans = default;

            conn.Open();

            try
            {
                trans = conn.BeginTransaction();

                using (var writer = conn.BeginBinaryImport(copy.Cmd))
                {
                    foreach (DataRow row in dt.Rows)
                    {
                        writer.StartRow();

                        foreach (var item in copy.Columns)
                        {
                            writer.Write(row[item.Key], item.Value);
                        }
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
        }

        public override void Insert(string spName, DataTable dt) => throw new Exception("This method is not supported");

        public override bool Exists(string tableName) => Count("pg_tables", new { schemaname = "public", tablename = tableName }) > 0;

        public override bool Create(string tableName, string[] definitions, bool exists)
        {
            return TryQuery($"CREATE TABLE{(exists ? " IF NOT EXIST" : "")} {tableName} {QueryBuilder.CreateTableDefinitions(definitions)}");
        }

        public override bool Drop(string tableName, bool exists)
        {
            return TryQuery($"DROP TABLE {(exists ? "IF EXISTS" : "")} {tableName}");
        }

        public override bool Truncate(string tableName, bool resetIdentity)
        {
            return TryQuery($"TRUNCATE TABLE {tableName}{(resetIdentity ? " RESTART IDENTITY" : "")}");
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

    public class PgQueryBuilder : QueryBuilder
    {
        protected override string CreateParameterName(string name) => $"${Params.Count + 1}";

        protected override string SelectStart(string sql) => sql;

        protected override string SelectEnd(string sql)
        {
            if (IsLimited(out int limit))
            {
                sql += $" LIMIT {limit}";
            }

            if (IsStarted(out int offset))
            {
                sql += $" OFFSET {offset}";
            }

            return sql;
        }
    }
}
