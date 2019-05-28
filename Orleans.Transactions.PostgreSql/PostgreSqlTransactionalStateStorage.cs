using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Npgsql;
using Orleans.Transactions.Abstractions;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace Orleans.Transactions.PostgreSql
{
    public class PostgreSqlTransactionalStateStorage<TState> : TransactionalStateStorage<TState>
        where TState : class, new()
    {
        private class TransactionMetadata : ITransactionMetadataEntity
        {
            public string ETag { get; set; }
            public long CommittedSequenceId { get; set; }
            public TransactionalStateMetaData Value { get; set; }
        }

        private class TransactionState : ITransactionStateEntity<TState>
        {
            public long SequenceId { get; set; }
            public string TransactionId { get; set; }
            public DateTimeOffset Timestamp { get; set; }
            public ParticipantId? TransactionManager { get; set; }
            public TState Value { get; set; }

            public void ClearValue()
            {
                Value = null;
            }
        }

        private readonly string _stateId;
        private readonly PostgreSqlTransactionalStateOptions _options;
        private readonly JsonSerializerSettings _jsonSettings;

        public PostgreSqlTransactionalStateStorage(StateReference stateReference,
            PostgreSqlTransactionalStateOptions options,
            JsonSerializerSettings jsonSettings)
        {
            _stateId = stateReference.ToString();
            _options = options;
            _jsonSettings = jsonSettings;
            _jsonSettings.TypeNameHandling = TypeNameHandling.Auto;
            _jsonSettings.DefaultValueHandling = DefaultValueHandling.Include;
            _jsonSettings.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter());
        }

        protected override Task<ITransactionMetadataEntity> ReadMetadata()
            => ExecuteQuery<ITransactionMetadataEntity>(async db =>
            {
                var transactionMetadata = await db.Query(_options.MetadataTableName)
                    .Where("state_id", _stateId)
                    .FirstOrDefaultAsync().ConfigureAwait(false);
                if (transactionMetadata == null)
                {
                    return new TransactionMetadata();
                }

                return new TransactionMetadata
                {
                    ETag = transactionMetadata.etag,
                    CommittedSequenceId = transactionMetadata.committed_sequence_id,
                    Value = JsonConvert.DeserializeObject<TransactionalStateMetaData>(transactionMetadata.value,
                        _jsonSettings)
                };
            });

        protected override Task<ITransactionStateEntity<TState>[]> ReadStates(long fromSequenceId)
            => ExecuteQuery<ITransactionStateEntity<TState>[]>(async db =>
            {
                var results = await db.Query(_options.StateTableName)
                    .Where("state_id", _stateId)
                    .Where("sequence_id", ">=", fromSequenceId)
                    .Select("sequence_id", "transaction_id", "transaction_manager", "value", "timestamp",
                        "transaction_id")
                    .GetAsync().ConfigureAwait(false);

                // ReSharper disable once CoVariantArrayConversion
                return results.Select(x => new TransactionState
                {
                    SequenceId = x.sequence_id,
                    TransactionId = x.transaction_id,
                    Timestamp = x.timestamp,
                    Value = JsonConvert.DeserializeObject<TState>(x.value, _jsonSettings),
                    TransactionManager =
                        JsonConvert.DeserializeObject<ParticipantId?>(x.transaction_manager, _jsonSettings)
                }).ToArray();
            });

        protected override Task<ITransactionStateEntity<TState>> PersistState(
            PendingTransactionState<TState> pendingState,
            long? commitUpTo,
            ITransactionStateEntity<TState> existingState = null) => ExecuteQuery<ITransactionStateEntity<TState>>(
            async db =>
            {
                var transactionManager =
                    JsonConvert.SerializeObject(pendingState.TransactionManager, _jsonSettings);
                var stateValue = pendingState.State != null
                    ? JsonConvert.SerializeObject(pendingState.State, _jsonSettings)
                    : null;

                if (existingState == null)
                {
                    await db.Query(_options.StateTableName).AsInsert(new[]
                        {
                            "state_id", "sequence_id", "transaction_manager", "value", "timestamp", "transaction_id"
                        },
                        new object[]
                        {
                            _stateId,
                            pendingState.SequenceId,
                            transactionManager,
                            stateValue,
                            pendingState.TimeStamp,
                            pendingState.TransactionId
                        }).FirstOrDefaultAsync();
                }
                else
                {
                    var rowsUpdated = await db.Query(_options.StateTableName)
                        .Where("state_id", _stateId)
                        .Where("sequence_id", existingState.SequenceId)
                        .AsUpdate(new[] {"transaction_manager", "value", "timestamp", "transaction_id"}, new object[]
                        {
                            transactionManager,
                            stateValue,
                            pendingState.TimeStamp,
                            pendingState.TransactionId
                        }).FirstOrDefaultAsync<int>();

                    if (rowsUpdated != 1)
                        throw new InvalidOperationException("Something went wrong while persisting existing state");
                }

                return new TransactionState
                {
                    Value = pendingState.State,
                    SequenceId = pendingState.SequenceId,
                    TransactionId = pendingState.TransactionId,
                    TransactionManager = pendingState.TransactionManager,
                    Timestamp = pendingState.TimeStamp
                };
            });

        protected override Task RemoveAbortedState(ITransactionStateEntity<TState> state)
            => ExecuteQuery(async db =>
            {
                var rowsDeleted = await db.Query(_options.StateTableName)
                    .Where("state_id", _stateId)
                    .Where("sequence_id", state.SequenceId)
                    .DeleteAsync().ConfigureAwait(false);

                Console.WriteLine(rowsDeleted);

                if (rowsDeleted != 1)
                    throw new InvalidOperationException("Something went wrong when trying to delete transaction state");
            });

        protected override Task<ITransactionMetadataEntity> PersistMetadata(TransactionalStateMetaData value,
            long commitSequenceId) => ExecuteQuery<ITransactionMetadataEntity>(async db =>
        {
            var tableName = _options.MetadataTableName;

            var newEtag = Guid.NewGuid().ToString();
            var serializedValue = JsonConvert.SerializeObject(value, _jsonSettings);

            if (Metadata.ETag == null)
            {
                await db.Query(tableName)
                    .AsInsert(new[] {"state_id", "committed_sequence_id", "etag", "value"},
                        new object[]
                        {
                            _stateId, commitSequenceId, newEtag, serializedValue
                        }).FirstOrDefaultAsync().ConfigureAwait(false);
            }
            else
            {
                var rowsUpdated = await db.Query(tableName).Where("state_id", _stateId).Where("etag", Metadata.ETag)
                    .UpdateAsync(new
                    {
                        committed_sequence_id = commitSequenceId,
                        etag = newEtag,
                        value = serializedValue
                    }).ConfigureAwait(false);

                if (rowsUpdated != 1)
                {
                    throw new InvalidOperationException("Could not update metadata. Possible concurrency issue");
                }
            }

            return new TransactionMetadata
            {
                Value = value,
                ETag = newEtag,
                CommittedSequenceId = commitSequenceId
            };
        });

        private Task ExecuteQuery(Func<QueryFactory, Task> execute) => ExecuteQuery<object>(async db =>
        {
            await execute(db);
            return null;
        });


        private async Task<TResult> ExecuteQuery<TResult>(Func<QueryFactory, Task<TResult>> execute)
        {
            using (var connection = new NpgsqlConnection(_options.ConnectionString))
            {
                await connection.OpenAsync();
                var compiler = new PostgresCompiler();
                var db = new QueryFactory(connection, compiler);
                return await execute(db);
            }
        }
    }
}