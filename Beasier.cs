using Ceasier.Configuration;
using Ceasier.Sap;
using Ceasier.Sql;
using Ceasier.Sql.Driver;
using Microsoft.Extensions.Configuration;
using SAP.Middleware.Connector;
using System;
using System.Collections.Generic;

namespace Ceasier
{
    public class Beasier<T> where T : class
    {
        public readonly T Value;
        public readonly IConfiguration Configuration;
        private readonly Dictionary<string, Db> Connections = new Dictionary<string, Db>();

        public Beasier() : this("appsettings.json")
        {
        }

        public Beasier(string filename)
        {
            Configuration = new ConfigurationBuilder().AddJsonFile(filename).Build();
            Value = Configuration.Get<T>();
        }

        public string GetDsn(string connectionName) => Configuration.GetConnectionString(connectionName);

        public Db GetDb(IDriver driver, string connectionName)
        {
            if (!Connections.ContainsKey(connectionName))
            {
                Connections[connectionName] = new Db(driver, GetDsn(connectionName));
            }
            
            return Connections[connectionName];
        }

        public Db GetMssql(string connectionName) => GetDb(new Mssql(), connectionName);

        public Db GetPgsql(string connectionName) => GetDb(new Pgsql(), connectionName);

        public RFCActor GetRFCActor(string name) => Configuration.GetSection($"RFCActors:{name}").Get<RFCActor>() ?? throw new Exception($"RFC Actor not found: {name}");

        public RFCConnection GetRFCConnection(string name) => Configuration.GetSection($"RFCConnections:{name}").Get<RFCConnection>() ?? throw new Exception($"RFC connection not found: {name}");

        public RFCConnection GetRFCConnection(string name, string actor) => GetRFCConnection(name).UseActor(GetRFCActor(actor));

        public RFCConnection GetRFCConnection(string name, string username, string password) => GetRFCConnection(name).UseActor(username, password);

        public RfcFun GetRFCFunction(string name, string profileName, string actorName) => new RfcFun(name, GetRFCConfiguration(profileName, actorName));

        public RfcConfigParameters GetRFCConfiguration(string connectionName, string actorName) => GetRFCConfiguration(GetRFCConnection(connectionName, actorName));

        public RfcConfigParameters GetRFCConfiguration(string connectionName, string username, string password) => GetRFCConfiguration(GetRFCConnection(connectionName, username, password));

        public RfcConfigParameters GetRFCConfiguration(RFCConnection profile) => new RfcConfigParameters
        {
            { RfcConfigParameters.Name, profile.Name },
            { RfcConfigParameters.AppServerHost, profile.AppServerHost },
            { RfcConfigParameters.SAPRouter, profile.SAPRouter },
            { RfcConfigParameters.Client, profile.Client },
            { RfcConfigParameters.User, profile.Actor.User },
            { RfcConfigParameters.Password, profile.Actor.Password },
            { RfcConfigParameters.SystemNumber, profile.SystemNumber },
            { RfcConfigParameters.Language, profile.Language },
            { RfcConfigParameters.PoolSize, profile.PoolSize }
        };
    }

    public class Beasier : Beasier<Project>
    {
    }
}
