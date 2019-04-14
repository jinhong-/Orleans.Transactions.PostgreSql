using System;
using System.Data;
using System.Globalization;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions.PostgreSql
{
    public class PostgreSqlTransactionalStateStorageFactory : ITransactionalStateStorageFactory
    {
        static PostgreSqlTransactionalStateStorageFactory()
        {
            //SqlMapper.AddTypeHandler(typeof(ParticipantId), new TestTypeHandler());
            //SqlMapper.SetTypeMap(typeof(TransactionStateEntity), new SnakeCaseMap<TransactionStateEntity>());
            //SqlMapper.SetTypeMap(typeof(TransactionMetadataEntity), new SnakeCaseMap<TransactionMetadataEntity>());
        }

        private readonly string _name;
        private readonly PostgreSqlTransactionalStateOptions _options;
        private readonly JsonSerializerSettings _jsonSettings;

        public PostgreSqlTransactionalStateStorageFactory(string name, PostgreSqlTransactionalStateOptions options,
            ITypeResolver typeResolver, IGrainFactory grainFactory)
        {
            _name = name;
            _options = options;
            _jsonSettings = TransactionalStateFactory.GetJsonSerializerSettings(
                typeResolver,
                grainFactory);
        }

        public static ITransactionalStateStorageFactory Create(IServiceProvider services, string name)
        {
            IOptionsSnapshot<PostgreSqlTransactionalStateOptions> optionsSnapshot =
                services.GetRequiredService<IOptionsSnapshot<PostgreSqlTransactionalStateOptions>>();
            return ActivatorUtilities.CreateInstance<PostgreSqlTransactionalStateStorageFactory>(services, name,
                optionsSnapshot.Get(name));
        }

        public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainActivationContext context)
            where TState : class, new()
        {
            var stateId = MakeStateId(context, stateName);
            return ActivatorUtilities.CreateInstance<PostgreSqlTransactionalStateStorage<TState>>(
                context.ActivationServices, stateId, _options, _jsonSettings);
        }

        protected virtual string MakeStateId(IGrainActivationContext context, string stateName)
        {
            string grainKey = context.GrainInstance.GrainReference.ToShortKeyString();
            var key = $"{grainKey}_{stateName}";
            return key;
        }
    }
}