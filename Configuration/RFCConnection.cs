namespace Ceasier.Configuration
{
    public class RFCConnection
    {
        public string Name { get; set; }

        public string AppServerHost { get; set; }

        public string SAPRouter { get; set; }

        public string Client { get; set; }

        public string Language { get; set; }

        public string SystemNumber { get; set; }

        public string PoolSize { get; set; }

        public RFCActor Actor { get; set; }

        public RFCConnection UseActor(RFCActor actor)
        {
            return new RFCConnection()
            {
                Name = this.Name,
                AppServerHost = this.AppServerHost,
                SAPRouter = this.SAPRouter,
                Client = this.Client,
                Language = this.Language,
                SystemNumber = this.SystemNumber,
                PoolSize = this.PoolSize,
                Actor = actor,
            };
        }

        public RFCConnection UseActor(string username, string password)
        {
            return UseActor(new RFCActor()
            {
                User = username,
                Password = password,
            });
        }
    }
}
