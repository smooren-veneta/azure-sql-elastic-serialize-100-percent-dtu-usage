using Microsoft.Data.SqlClient;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace elastic_sql_pool_serializable_100_percent_dtu_utilization
{
    public static class QueryFactoryFactory
    {
        public static QueryFactory Create()
        {
            var server = "";

            var connection = new SqlConnection(
                    server);
            var compiler = new SqlServerCompiler();

            return new QueryFactory(connection, compiler);

        }
    }
}
