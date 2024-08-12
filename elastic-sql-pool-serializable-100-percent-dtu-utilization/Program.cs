using elastic_sql_pool_serializable_100_percent_dtu_utilization;
using SqlKata.Execution;
using System.Data;

internal class Program
{
    public static string Table = "dtuSerializableTest";
    static async Task Main(string[] args)
    {
        var queryFactory = QueryFactoryFactory.Create();
        queryFactory.Statement($"truncate table {Table}");

        var cancelationToken = new CancellationTokenSource();
        var consumer = new Consumer();
        var producers = Enumerable.Range(0, 50).Select(x => new Producer(x))
             .ToList();
        var tasks = new List<Task>();
        foreach (var producer in producers)
        {
            tasks.Add(producer.StartGeneratingAsync(cancelationToken.Token));
        }
        var lastPosition = 0;
        while (!cancelationToken.IsCancellationRequested)
        {
            var positions = await consumer.ReadNextAsync(lastPosition);
            for (int i = 0; i < positions.Count - 1; i++)
            {
                int current = positions[i];
                int next = positions[i + 1];
                if (next - current > 1)
                {
                    var validate = queryFactory.Query(Table)
                       .Select("Position")
                       .WhereIn("Position", Enumerable.Range(current + 1, next - current - 1))
                       .Get<int>()
                       .ToList();
                    if (validate.Count != 0)
                    {
                        Console.WriteLine($"skipped from current: {current} to next: {next}, values: {string.Join(", ", validate)}");
                    }
                }
            }
            lastPosition = positions.Max();
        }
    }
}
