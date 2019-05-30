using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using Orleans.Transactions.Abstractions;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace Orleans.Transactions.PostgreSql
{
    public class PostgreSqlTransactionalStateStorage2<TState> : ITransactionalStateStorage<TState>
        where TState : class, new()
    {
        private class TransactionMetadataEntity : ITransactionMetadataEntity
        {
            public string ETag { get; set; }
            public long CommittedSequenceId { get; set; }
            public TransactionalStateMetaData Value { get; set; }
        }

        private class TransactionStateEntity : ITransactionStateEntity<TState>
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
        private readonly ILogger<PostgreSqlTransactionalStateStorage2<TState>> _logger;
        private readonly DbExecuter _dbExecuter;
        private List<ITransactionStateEntity<TState>> _states;
        private ITransactionMetadataEntity _metadata;


        public PostgreSqlTransactionalStateStorage2(StateReference stateReference,
            PostgreSqlTransactionalStateOptions options,
            JsonSerializerSettings jsonSettings,
            ILogger<PostgreSqlTransactionalStateStorage2<TState>> logger)
        {
            _stateId = stateReference.ToString();
            _options = options;
            _dbExecuter = new DbExecuter(_options.ConnectionString);
            _jsonSettings = jsonSettings;
            _logger = logger;
            _jsonSettings.TypeNameHandling = TypeNameHandling.Auto;
            _jsonSettings.DefaultValueHandling = DefaultValueHandling.Include;
            _jsonSettings.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter());
        }

        public async Task<TransactionalStorageLoadResponse<TState>> Load()
        {
            var readMetadataTask = ReadMetadata().ConfigureAwait(false);
            var readStatesTask = ReadStates().ConfigureAwait(false);

            _metadata = await readMetadataTask;
            _states = (await readStatesTask).OrderBy(x => x.SequenceId).ToList();

            if (string.IsNullOrEmpty(_metadata.ETag))
            {
                return new TransactionalStorageLoadResponse<TState>();
            }

            TState committedState;
            if (_metadata.CommittedSequenceId == 0)
            {
                committedState = new TState();
            }
            else
            {
                if (!FindState(_metadata.CommittedSequenceId, out var pos))
                {
                    var error =
                        $"Storage state corrupted: no record for committed state v{_metadata.CommittedSequenceId}";
                    throw new InvalidOperationException(error);
                }

                committedState = _states[pos].Value;
            }

            var prepareRecordsToRecover = _states.Where(x => x.SequenceId > _metadata.CommittedSequenceId)
                .TakeWhile(x => x.TransactionManager.HasValue)
                .Select(x => new PendingTransactionState<TState>
                {
                    SequenceId = x.SequenceId,
                    TransactionManager = x.TransactionManager.Value,
                    State = x.Value,
                    TimeStamp = x.Timestamp.UtcDateTime,
                    TransactionId = x.TransactionId
                })
                .ToArray();

            // clear the state value... no longer needed, ok to GC now
            foreach (var state in _states)
            {
                state.ClearValue();
            }

            var metadata = _metadata.Value;
            return new TransactionalStorageLoadResponse<TState>(_metadata.ETag, committedState,
                _metadata.CommittedSequenceId, metadata, prepareRecordsToRecover);
        }

        public async Task<string> Store(string expectedETag, TransactionalStateMetaData metadata,
            List<PendingTransactionState<TState>> statesToPrepare, long? commitUpTo,
            long? abortAfter)
        {
            if (_metadata.ETag != expectedETag)
                throw new ArgumentException(nameof(expectedETag), "Etag does not match");

            var stateUpdater = new StateUpdater(_dbExecuter, _stateId, _options.StateTableName, _jsonSettings);

            // first, clean up aborted records
            if (abortAfter.HasValue && _states.Count != 0)
            {
                await stateUpdater.DeleteStates(afterSequenceId: abortAfter);
                while (_states.Count > 0 && _states[_states.Count - 1].SequenceId > abortAfter)
                {
                    _states.RemoveAt(_states.Count - 1);
                }
            }

            // second, persist non-obsolete prepare records
            var obsoleteBefore = commitUpTo.HasValue ? commitUpTo.Value : _metadata.CommittedSequenceId;
            if (statesToPrepare != null && statesToPrepare.Count > 0)
                foreach (var s in statesToPrepare)
                    if (s.SequenceId >= obsoleteBefore)
                    {
                        var newState = new TransactionStateEntity
                        {
                            SequenceId = s.SequenceId,
                            TransactionId = s.TransactionId,
                            Timestamp = s.TimeStamp,
                            TransactionManager = s.TransactionManager,
                            Value = s.State
                        };

                        if (FindState(s.SequenceId, out var pos))
                        {
                            // overwrite with new pending state
                            _states[pos] = newState;
                            await stateUpdater.UpdateState(newState).ConfigureAwait(false);
                        }
                        else
                        {
                            await stateUpdater.InsertState(newState).ConfigureAwait(false);
                            _states.Insert(pos, newState);
                        }

                        _logger.LogTrace($"{_stateId}.{newState.SequenceId} Update {newState.TransactionId}");
                    }

            await stateUpdater.EnsureInsertBufferFlushed();

            // third, persist metadata and commit position
            _metadata = await PersistMetadata(metadata, commitUpTo ?? _metadata.CommittedSequenceId);

            // fourth, remove obsolete records
            if (_states.Count > 0 && _states[0].SequenceId < obsoleteBefore)
            {
                FindState(obsoleteBefore, out var pos);
                await stateUpdater.DeleteStates(beforeSequenceId: obsoleteBefore);
                _states.RemoveRange(0, pos);
            }

            return _metadata.ETag;
        }

        private bool FindState(long sequenceId, out int pos)
        {
            pos = 0;
            foreach (var stateSequenceId in _states.Select(x => x.SequenceId))
            {
                switch (stateSequenceId.CompareTo(sequenceId))
                {
                    case 0:
                        return true;
                    case -1:
                        pos++;
                        continue;
                    case 1:
                        return false;
                }
            }

            return false;
        }

        private Task<ITransactionMetadataEntity> ReadMetadata()
            => _dbExecuter.ExecuteQuery<ITransactionMetadataEntity>(async db =>
            {
                var transactionMetadata = await db.Query(_options.MetadataTableName)
                    .Where("state_id", _stateId)
                    .FirstOrDefaultAsync().ConfigureAwait(false);
                if (transactionMetadata == null)
                {
                    return new TransactionMetadataEntity();
                }

                return new TransactionMetadataEntity
                {
                    ETag = transactionMetadata.etag,
                    CommittedSequenceId = transactionMetadata.committed_sequence_id,
                    Value = JsonConvert.DeserializeObject<TransactionalStateMetaData>(transactionMetadata.value,
                        _jsonSettings)
                };
            });

        private Task<ITransactionMetadataEntity> PersistMetadata(TransactionalStateMetaData value,
            long commitSequenceId) => _dbExecuter.ExecuteQuery<ITransactionMetadataEntity>(async db =>
        {
            var tableName = _options.MetadataTableName;

            var newEtag = Guid.NewGuid().ToString();
            var serializedValue = JsonConvert.SerializeObject(value, _jsonSettings);

            if (_metadata.ETag == null)
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
                var rowsUpdated = await db.Query(tableName).Where("state_id", _stateId).Where("etag", _metadata.ETag)
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

            return new TransactionMetadataEntity
            {
                Value = value,
                ETag = newEtag,
                CommittedSequenceId = commitSequenceId
            };
        });

        private Task<ITransactionStateEntity<TState>[]> ReadStates()
            => _dbExecuter.ExecuteQuery<ITransactionStateEntity<TState>[]>(async db =>
            {
                var results = await db.Query(_options.StateTableName)
                    .Where("state_id", _stateId)
                    .Select("sequence_id", "transaction_id", "transaction_manager", "value", "timestamp",
                        "transaction_id")
                    .GetAsync().ConfigureAwait(false);

                // ReSharper disable once CoVariantArrayConversion
                return results.Select(x => new TransactionStateEntity
                {
                    SequenceId = x.sequence_id,
                    TransactionId = x.transaction_id,
                    Timestamp = x.timestamp,
                    Value = JsonConvert.DeserializeObject<TState>(x.value, _jsonSettings),
                    TransactionManager =
                        JsonConvert.DeserializeObject<ParticipantId?>(x.transaction_manager, _jsonSettings)
                }).ToArray();
            });


        private class DbExecuter
        {
            private readonly string _connectionString;

            public DbExecuter(string connectionString)
            {
                _connectionString = connectionString;
            }

            public Task ExecuteQuery(Func<QueryFactory, Task> execute) => ExecuteQuery<object>(async db =>
            {
                await execute(db);
                return null;
            });

            public async Task<TResult> ExecuteQuery<TResult>(Func<QueryFactory, Task<TResult>> execute)
            {
                using (var connection = new NpgsqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    var compiler = new PostgresCompiler();
                    var db = new QueryFactory(connection, compiler);
                    return await execute(db);
                }
            }
        }

        private class StateUpdater
        {
            private readonly DbExecuter _dbExecuter;
            private readonly string _stateId;
            private readonly string _stateTableName;
            private readonly JsonSerializerSettings _jsonSettings;

            private readonly List<ITransactionStateEntity<TState>> _insertStateBuffer =
                new List<ITransactionStateEntity<TState>>();

            public StateUpdater(DbExecuter dbExecuter, string stateId, string stateTableName,
                JsonSerializerSettings jsonSettings)
            {
                _dbExecuter = dbExecuter;
                _stateId = stateId;
                _stateTableName = stateTableName;
                _jsonSettings = jsonSettings;
            }

            public async Task DeleteStates(long? beforeSequenceId = null, long? afterSequenceId = null)
            {
                if (!beforeSequenceId.HasValue && !afterSequenceId.HasValue)
                    throw new InvalidOperationException(
                        $"{nameof(beforeSequenceId)} or {nameof(afterSequenceId)} must be provided");
                await EnsureInsertBufferFlushed();

                await _dbExecuter.ExecuteQuery(async db =>
                {
                    var query = db.Query(_stateTableName)
                        .Where("state_id", _stateId);

                    if (beforeSequenceId.HasValue)
                    {
                        query = query.Where("sequence_id", "<", beforeSequenceId.Value);
                    }

                    if (afterSequenceId.HasValue)
                    {
                        query = query.Where("sequence_id", ">", afterSequenceId.Value);
                    }

                    await query.DeleteAsync().ConfigureAwait(false);
                });
            }

            public async Task UpdateState(ITransactionStateEntity<TState> state)
            {
                await EnsureInsertBufferFlushed();
                await _dbExecuter.ExecuteQuery(async db =>
                {
                    var (stateValue, transactionManager) = GetSerializables(state);

                    var rowsUpdated = await db.Query(_stateTableName)
                        .Where("state_id", _stateId)
                        .Where("sequence_id", state.SequenceId)
                        .AsUpdate(new[] {"transaction_manager", "value", "timestamp", "transaction_id"}, new object[]
                        {
                            transactionManager,
                            stateValue,
                            state.Timestamp,
                            state.TransactionId
                        })
                        .FirstOrDefaultAsync<int>().ConfigureAwait(false);
                    if (rowsUpdated != 1)
                        throw new InvalidOperationException("Something went wrong while persisting existing state");
                });
            }

            public async Task InsertState(ITransactionStateEntity<TState> state)
            {
                _insertStateBuffer.Add(state);

                if (_insertStateBuffer.Count <= 1000) return;
                await EnsureInsertBufferFlushed();
            }

            public async Task EnsureInsertBufferFlushed()
            {
                if (_insertStateBuffer.Count == 0) return;

                await _dbExecuter.ExecuteQuery(async db =>
                {
                    await db.Query(_stateTableName).AsInsert(new[]
                            {
                                "state_id", "sequence_id", "transaction_manager", "value", "timestamp", "transaction_id"
                            },
                            _insertStateBuffer.Select(CreatePropertyBagForInsert).ToArray())
                        .FirstOrDefaultAsync().ConfigureAwait(false);;
                });

                _insertStateBuffer.Clear();
            }

            private object[] CreatePropertyBagForInsert(ITransactionStateEntity<TState> state)
            {
                var (stateValue, transactionManager) = GetSerializables(state);
                return new object[]
                {
                    _stateId,
                    state.SequenceId,
                    transactionManager,
                    stateValue,
                    state.Timestamp,
                    state.TransactionId
                };
            }

            private (string StateValue, string TransactionManager) GetSerializables(
                ITransactionStateEntity<TState> state)
            {
                var transactionManager =
                    JsonConvert.SerializeObject(state.TransactionManager, _jsonSettings);
                var stateValue = state.Value != null
                    ? JsonConvert.SerializeObject(state.Value, _jsonSettings)
                    : null;

                return (stateValue, transactionManager);
            }
        }
    }
}