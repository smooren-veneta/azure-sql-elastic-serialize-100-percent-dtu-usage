using SqlKata.Execution;
using System.Data;

namespace elastic_sql_pool_serializable_100_percent_dtu_utilization
{
    internal class Consumer
    {
        private readonly QueryFactory _queryFactory;

        public Consumer()
        {
            _queryFactory = QueryFactoryFactory.Create();
        }

        public async Task<List<int>> ReadNextAsync(int lastPosition)
        {
            var events = new List<int>();

            _queryFactory.Connection.Open();
            using var scope = _queryFactory.Connection.BeginTransaction(IsolationLevel.Serializable);
            try
            {
                var queryResult = await _queryFactory.Query(Program.Table)
                    .Select("Position")
                    .Where("Position", ">=", lastPosition)
                    .OrderBy("Position")
                    .Limit(200)
                    .GetAsync<int>(scope);
                events = queryResult.ToList();
                scope.Commit();
            }
            catch (Exception)
            {
                scope.Rollback();
                throw;
            }
            finally
            {
                _queryFactory.Connection.Close();
            }
            return events;
        }
    }
}
