using System;
using System.Globalization;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions.PostgreSql
{
    public class PostgreSqlTransactionalStateStorageFactory : ITransactionalStateStorageFactory
    {
        static PostgreSqlTransactionalStateStorageFactory()
        {
            SqlMapper.SetTypeMap(typeof(TransactionStateEntity), new SnakeCaseMap<TransactionStateEntity>());
            SqlMapper.SetTypeMap(typeof(TransactionMetadataEntity), new SnakeCaseMap<TransactionMetadataEntity>());
        }

        private readonly string _name;
        private readonly PostgreSqlTransactionalStateOptions _options;

        public PostgreSqlTransactionalStateStorageFactory(string name, PostgreSqlTransactionalStateOptions options)
        {
            _name = name;
            _options = options;
        }
        
        public static ITransactionalStateStorageFactory Create(IServiceProvider services, string name)
        {
            IOptionsSnapshot<PostgreSqlTransactionalStateOptions> optionsSnapshot = services.GetRequiredService<IOptionsSnapshot<PostgreSqlTransactionalStateOptions>>();
            return ActivatorUtilities.CreateInstance<PostgreSqlTransactionalStateStorageFactory>(services, name, optionsSnapshot.Get(name));
        }

        public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainActivationContext context)
            where TState : class, new()
        {
            var stateId = MakeStateId(context, stateName);
            return ActivatorUtilities.CreateInstance<PostgreSqlTransactionalStateStorage<TState>>(
                context.ActivationServices, stateId, _options);
        }

        protected virtual string MakeStateId(IGrainActivationContext context, string stateName)
        {
            string grainKey = context.GrainInstance.GrainReference.ToShortKeyString();
            var key = $"{grainKey}_{stateName}";
            return key;
        }
    }
}