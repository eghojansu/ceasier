using System.Data.Common;
using System.Data.SqlClient;
using System.Data;
using System;

namespace Ceasier.Sql
{
    public class Mssql: Db<SqlDataReader>
    {
        public Mssql(string connectionString) : base(connectionString)
        {
        }

        protected override DbConnection CreateConnection(string Dsn) => new SqlConnection(Dsn);

        protected override DbParameter CreateParameterFor(DbCommand cmd, string name, object value)
        {
            if (value is SqlParameter param)
            {
                return param;
            }

            return new SqlParameter(name, value);
        }

        public override QueryBuilder Qb() => new MsQueryBuilder();

        public override DbCommand CreateCommand(string commandText, CommandType commandType, DbTransaction transaction) => new SqlCommand()
        {
            Connection = Connection as SqlConnection,
            CommandText = commandText,
            CommandType = commandType,
            Transaction = transaction as SqlTransaction,
        };

        public override void Insert(DataTable dt)
        {
            var conn = Connection as SqlConnection;

            SqlTransaction trans = default;

            conn.Open();

            try
            {
                trans = conn.BeginTransaction();

                using (var tbl = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, trans))
                {
                    tbl.DestinationTableName = dt.TableName;

                    foreach (DataColumn column in dt.Columns)
                    {
                        tbl.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }

                    tbl.WriteToServer(dt);
                }

                trans.Commit();
            }
            catch
            {
                trans?.Rollback();

                throw;
            }
        }

        public override void Insert(string spName, DataTable dt)
        {
            var conn = Connection as SqlConnection;

            using (var cmd = new SqlCommand(spName, conn))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue(dt.TableName, dt);

                conn.Open();

                cmd.ExecuteNonQuery();
            }
        }

        public override bool Exists(string tableName)
        {
            return Query<int>($"SELECT ISNULL(OBJECT_ID(@tableName, 'U'), 0) c", new { tableName }, true) > 0;
        }

        public override bool Create(string tableName, string[] definitions, bool exists)
        {
            return (exists && Exists(tableName)) || TryQuery($"CREATE TABLE {tableName} {QueryBuilder.CreateTableDefinitions(definitions)}");
        }

        public override bool Drop(string tableName, bool exists)
        {
            return (exists && !Exists(tableName)) || TryQuery($"DROP TABLE {tableName}");
        }

        public override bool Truncate(string tableName, bool resetIdentity)
        {
            if (resetIdentity)
            {
                throw new Exception("Truncate with restart identiy not supported");
            }

            return TryQuery($"TRUNCATE TABLE {tableName}");
        }
    }

    public class MsQueryBuilder : QueryBuilder
    {
        protected override string CreateParameterName(string name) => $"@{name}";

        protected override string SelectStart(string sql)
        {
            if (IsLimited(out int limit) && !IsStarted())
            {
                sql += $" TOP {limit}";
            }

            return sql;
        }

        protected override string SelectEnd(string sql)
        {
            if (IsLimited(out int limit) && IsStarted(out int offset))
            {
                sql += $" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY";
            }

            return sql;
        }
    }
}
