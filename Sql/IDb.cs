using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Ceasier.Sql
{
    public interface IDb
    {
        DbConnection GetConnection();

        T Run<T>(DbCommand cmd, object parameters, bool scalar);

        T Run<T>(string cmd, object parameters, bool scalar, CommandType commandType);

        bool TryRun(string cmd, object parameters);

        DbDataReader Run(string cmd, object parameters, CommandType commandType);

        List<Dictionary<string, object>> Result(string cmd, object parameters, CommandType commandType);

        Dictionary<string, object> ResultFirst(string cmd, object parameters, CommandType commandType);

        T Query<T>(string cmd, object parameters, bool scalar);

        DbDataReader Query(string cmd, object parameters);

        List<Dictionary<string, object>> QueryResult(string cmd, object parameters);

        Dictionary<string, object> QueryFirst(string cmd, object parameters);

        T Sp<T>(string name, object parameters, bool scalar);

        DbDataReader Sp(string name, object parameters);

        List<Dictionary<string, object>> SpResult(string name, object parameters);

        Dictionary<string, object> SpFirst(string name, object parameters);

        T Fn<T>(string name, object parameters, bool scalar);

        DbDataReader Fn(string name, object parameters);

        List<Dictionary<string, object>> FnResult(string name, object parameters);

        Dictionary<string, object> FnFirst(string name, object parameters);

        DbDataReader Read(string table, object filters, Dictionary<string, object> options);

        List<Dictionary<string, object>> Find(string table, object filters, Dictionary<string, object> options);

        Dictionary<string, object> First(string table, object filters, Dictionary<string, object> options);

        int Count(string table, object filters, Dictionary<string, object> options);

        Dictionary<string, object> ReadRow(DbDataReader reader);

        List<Dictionary<string, object>> ReadRows(DbDataReader reader, bool close);

        int Insert(string table, object data);

        void Insert(DataTable dt);

        void Insert(string spName, DataTable dt);

        int Update(string table, object data, object filters);

        int Delete(string table, object filters);

        bool Exists(string tableName);

        bool Create(string tableName, string[] definitions, bool exists);

        bool Drop(string tableName, bool exists);

        bool Truncate(string tableName, bool resetIdentity);
    }
}
