namespace Orleans.Transactions.PostgreSql
{
    public class PostgreSqlTransactionalStateOptions
    {
        public string ConnectionString { get; set; }
        public string StateTableName { get; set; }
        public string KeyTableName { get; set; }
    }
}