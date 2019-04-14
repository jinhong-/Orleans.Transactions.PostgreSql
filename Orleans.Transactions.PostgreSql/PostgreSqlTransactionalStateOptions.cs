using Newtonsoft.Json;

namespace Orleans.Transactions.PostgreSql
{
    public class PostgreSqlTransactionalStateOptions
    {
        public string ConnectionString { get; set; }
        public string StateTableName { get; set; } = "transaction_state";
        public string MetadataTableName { get; set; } = "transaction_metadata";

        public JsonSerializerSettings JsonSerializerSettings { get; set; } = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Auto
        };
    }
}