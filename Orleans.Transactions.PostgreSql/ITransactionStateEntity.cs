using System;

namespace Orleans.Transactions.PostgreSql
{
    public interface ITransactionStateEntity<out TState>
    {
        long SequenceId { get; }
        string TransactionId { get; }
        DateTimeOffset Timestamp { get; }
        ParticipantId? TransactionManager { get; }
        TState Value { get; }
        void ClearValue();
    }
}