using System.Data.Common;
using System.Data;

namespace Ceasier.Sql
{
    public interface IDriver
    {
        DbConnection CreateConnection(string Dsn);

        DbCommand CreateCommand(IDb db, string commandText, CommandType commandType, DbTransaction transaction);

        DbDataReader GetReader(IDb db, string cmd, object parameters, CommandType commandType);

        DbParameter CreateParameterFor(DbCommand cmd, string name, object value);

        string CreateParameterName(QueryBuilder qb, string name);

        bool SupportTableParameter();

        void TableInsert(IDb db, DataTable dt);

        bool TableExists(IDb db, string tableName);

        bool TableCreate(IDb db, string tableName, string[] definitions, bool exists);

        bool TableDrop(IDb db, string tableName, bool exists);

        bool TableTruncate(IDb db, string tableName, bool resetIdentity);

        string QuerySelectStart(QueryBuilder qb, string cmd);

        string QuerySelectEnd(QueryBuilder qb, string cmd);
    }
}
