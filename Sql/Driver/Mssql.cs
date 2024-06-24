using System.Data.Common;
using System.Data.SqlClient;
using System.Data;
using System;

namespace Ceasier.Sql.Driver
{
    public class Mssql : IDriver
    {
        public DbConnection CreateConnection(string Dsn) => new SqlConnection(Dsn);

        public DbCommand CreateCommand(IDb db, string commandText, CommandType commandType, DbTransaction transaction)
        {
            return new SqlCommand()
            {
                Connection = db.GetConnection() as SqlConnection,
                CommandText = commandText,
                CommandType = commandType,
                Transaction = transaction as SqlTransaction,
            };
        }

        public DbDataReader GetReader(IDb db, string cmd, object parameters, CommandType commandType)
        {
            return db.Run<SqlDataReader>(cmd, parameters, false, commandType);
        }

        public DbParameter CreateParameterFor(DbCommand cmd, string name, object value)
        {
            if (value is SqlParameter param)
            {
                return param;
            }

            return new SqlParameter(name, value);
        }

        public string CreateParameterName(QueryBuilder qb, string name) => $"@{name}";

        public bool SupportTableParameter() => true;

        public void TableInsert(IDb db, DataTable dt)
        {
            var conn = db.GetConnection() as SqlConnection;

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

        public bool TableExists(IDb db, string tableName)
        {
            return db.Query<int>($"SELECT ISNULL(OBJECT_ID(@tableName, 'U'), 0) c", new { tableName }, true) > 0;
        }

        public bool TableCreate(IDb db, string tableName, string[] definitions, bool exists)
        {
            return (exists && TableExists(db, tableName)) || db.TryRun($"CREATE TABLE {tableName} {QueryBuilder.CreateTableDefinitions(definitions)}", null);
        }

        public bool TableDrop(IDb db, string tableName, bool exists)
        {
            return (exists && !TableExists(db, tableName)) || db.TryRun($"DROP TABLE {tableName}", null);
        }

        public bool TableTruncate(IDb db, string tableName, bool resetIdentity)
        {
            if (resetIdentity)
            {
                throw new Exception("Truncate with restart identiy not supported");
            }

            return db.TryRun($"TRUNCATE TABLE {tableName}", null);
        }

        public string QuerySelectStart(QueryBuilder qb, string cmd)
        {
            if (qb.IsLimited(out int limit) && !qb.IsStarted())
            {
                cmd += $" TOP {limit}";
            }

            return cmd;
        }

        public string QuerySelectEnd(QueryBuilder qb, string cmd)
        {
            if (qb.IsLimited(out int limit) && qb.IsStarted(out int offset))
            {
                cmd += $" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY";
            }

            return cmd;
        }

        public bool IsConnectionError(Exception error)
        {
            return false;
        }
    }
}
