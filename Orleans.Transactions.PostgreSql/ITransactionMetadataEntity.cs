using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions.PostgreSql
{
    public interface ITransactionMetadataEntity
    {
        string ETag { get; }
        long CommittedSequenceId { get; }
        TransactionalStateMetaData Value { get; }
    }
}