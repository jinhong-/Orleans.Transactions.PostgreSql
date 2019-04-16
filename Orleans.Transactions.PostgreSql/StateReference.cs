using Orleans.Runtime;

namespace Orleans.Transactions.PostgreSql
{
    public class StateReference
    {
        public GrainReference GrainReference { get; }
        public string StateName { get; }

        public StateReference(GrainReference grainReference, string stateName)
        {
            GrainReference = grainReference;
            StateName = stateName;
        }

        public override string ToString()
        {
            string grainKey = GrainReference.ToShortKeyString();
            var key = $"{grainKey}_{StateName}";
            return key;
        }
    }
}