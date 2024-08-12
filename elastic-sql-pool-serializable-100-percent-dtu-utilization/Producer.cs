using SqlKata.Execution;

namespace elastic_sql_pool_serializable_100_percent_dtu_utilization
{
    internal class Producer
    {
        private readonly QueryFactory _queryFactory;
        private readonly int _processorId;

        public Producer(int processorId)
        {
            _queryFactory = QueryFactoryFactory.Create();
            _processorId = processorId;
        }

        public async Task StartGeneratingAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {

                _queryFactory.Connection.Open();
                using var scope = _queryFactory.Connection.BeginTransaction();
                try
                {
                    await _queryFactory.Query(Program.Table).InsertAsync(new
                    {
                        ProcessorId = _processorId
                    }, scope);
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
                await Task.Delay(20);
            }
        }
    }
}
