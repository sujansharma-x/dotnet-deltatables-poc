using System;
using System.Data;
using System.Data.Odbc;
using Microsoft.Extensions.Configuration;

namespace DatabricksDeltaCRUD
{
    public class DatabricksConnectionSettings
    {
        public string Host { get; set; } = string.Empty;
        public string HttpPath { get; set; } = string.Empty;
        public string Token { get; set; } = string.Empty;
        public string Catalog { get; set; } = string.Empty;
        public string Schema { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
    }

    public class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public int Quantity { get; set; }
    }

    public class DatabricksDeltaService
    {
        private readonly string _connectionString;
        private readonly string _tableName;

        public DatabricksDeltaService(string host, string httpPath, string token, string catalog, string schema, string tableName)
        {
            // databricks ODBC connection string
            _connectionString = $"Driver={{Simba Spark ODBC Driver}};" +
                              $"Host={host};" +
                              $"Port=443;" +
                              $"SSL=1;" +
                              $"ThriftTransport=2;" +
                              $"AuthMech=3;" +
                              $"UID=token;" +
                              $"PWD={token};" +
                              $"HTTPPath={httpPath}";
            
            _tableName = $"{catalog}.{schema}.{tableName}";
        }

        private OdbcConnection GetConnection()
        {
            return new OdbcConnection(_connectionString);
        }

        // helper method to escape SQL strings
        private string EscapeSqlString(string value)
        {
            return value.Replace("'", "''");
        }

        // create
        public void CreateProduct(Product product)
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                string query = $"INSERT INTO {_tableName} (id, name, price, quantity) " +
                             $"VALUES ({product.Id}, '{EscapeSqlString(product.Name)}', {product.Price}, {product.Quantity})";
                
                using (var cmd = new OdbcCommand(query, conn))
                {
                    cmd.ExecuteNonQuery();
                    Console.WriteLine($"Product created: {product.Name}");
                }
            }
        }

        // read - Single
        public Product? GetProduct(int id)
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                string query = $"SELECT id, name, price, quantity FROM {_tableName} WHERE id = {id}";
                
                using (var cmd = new OdbcCommand(query, conn))
                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return new Product
                        {
                            Id = reader.GetInt32(0),
                            Name = reader.GetString(1),
                            Price = reader.GetDecimal(2),
                            Quantity = reader.GetInt32(3)
                        };
                    }
                }
            }
            return null;
        }

        // read - All
        public void GetAllProducts()
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                string query = $"SELECT id, name, price, quantity FROM {_tableName}";
                
                using (var cmd = new OdbcCommand(query, conn))
                using (var reader = cmd.ExecuteReader())
                {
                    Console.WriteLine("\n--- All Products ---");
                    while (reader.Read())
                    {
                        Console.WriteLine($"ID: {reader.GetInt32(0)}, " +
                                        $"Name: {reader.GetString(1)}, " +
                                        $"Price: {reader.GetDecimal(2)}, " +
                                        $"Quantity: {reader.GetInt32(3)}");
                    }
                }
            }
        }

        // update
        public void UpdateProduct(Product product)
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                string query = $"UPDATE {_tableName} SET name = '{EscapeSqlString(product.Name)}', " +
                             $"price = {product.Price}, quantity = {product.Quantity} WHERE id = {product.Id}";
                
                using (var cmd = new OdbcCommand(query, conn))
                {
                    int rowsAffected = cmd.ExecuteNonQuery();
                    Console.WriteLine($"Product updated: {product.Name} (Rows affected: {rowsAffected})");
                }
            }
        }

        // delete
        public void DeleteProduct(int id)
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                string query = $"DELETE FROM {_tableName} WHERE id = {id}";
                
                using (var cmd = new OdbcCommand(query, conn))
                {
                    int rowsAffected = cmd.ExecuteNonQuery();
                    Console.WriteLine($"Product deleted: ID {id} (Rows affected: {rowsAffected})");
                }
            }
        }

        // initialize table (for testing)
        public void CreateTableIfNotExists()
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                
                // create table
                string query = $@"
                    CREATE TABLE IF NOT EXISTS {_tableName} (
                        id INT,
                        name STRING,
                        price DECIMAL(10,2),
                        quantity INT
                    ) USING DELTA";
                
                using (var cmd = new OdbcCommand(query, conn))
                {
                    cmd.ExecuteNonQuery();
                    Console.WriteLine($"Table '{_tableName}' created or already exists.");
                }
            }
        }
        
        // drop table if exists
        public void DropTableIfExists()
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                string query = $"DROP TABLE IF EXISTS {_tableName}";
                
                using (var cmd = new OdbcCommand(query, conn))
                {
                    cmd.ExecuteNonQuery();
                    Console.WriteLine($"Table '{_tableName}' dropped if it existed.");
                }
            }
        }
        
        
        // helper method to list available schemas
        public void ListSchemas(string catalog)
        {
            using (var conn = GetConnection())
            {
                conn.Open();
                string query = $"SHOW SCHEMAS IN {catalog}";
                
                using (var cmd = new OdbcCommand(query, conn))
                using (var reader = cmd.ExecuteReader())
                {
                    Console.WriteLine($"\n--- Available Schemas in {catalog} ---");
                    while (reader.Read())
                    {
                        Console.WriteLine($"Schema: {reader.GetString(0)}");
                    }
                }
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            // load config from appsettings.json
            var environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Development";
            
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile($"appsettings.{environment}.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var connectionSettings = new DatabricksConnectionSettings();
            configuration.GetSection("DatabricksConnection").Bind(connectionSettings);

            // validate config
            if (string.IsNullOrEmpty(connectionSettings.Host))
            {
                Console.WriteLine("Error: DatabricksConnection:Host is not configured in appsettings.json");
                return;
            }

            string host = connectionSettings.Host;
            string httpPath = connectionSettings.HttpPath;
            string token = connectionSettings.Token;
            string catalog = connectionSettings.Catalog;
            string schema = connectionSettings.Schema;
            string tableName = connectionSettings.TableName;

            try
            {
                var service = new DatabricksDeltaService(host, httpPath, token, catalog, schema, tableName);

                // list available catalogs and schemas
                Console.WriteLine("=== Checking available catalogs and schemas ===");
                try
                {
                    service.ListSchemas(catalog);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Could not list catalogs/schemas: {ex.Message}");
                }

                // drop and create table
                Console.WriteLine("\n=== Dropping Table if Exists ===");
                service.DropTableIfExists();
                
                Console.WriteLine("\n=== Creating/Verifying Table ===");
                service.CreateTableIfNotExists();

                // create
                Console.WriteLine("\n=== CREATE Operation ===");
                var product1 = new Product { Id = 1, Name = "Laptop", Price = 999.99m, Quantity = 10 };
                var product2 = new Product { Id = 2, Name = "Mouse", Price = 29.99m, Quantity = 50 };
                service.CreateProduct(product1);
                service.CreateProduct(product2);

                // read
                Console.WriteLine("\n=== READ Operation ===");
                service.GetAllProducts();

                // update
                Console.WriteLine("\n=== UPDATE Operation ===");
                product1.Price = 899.99m;
                product1.Quantity = 15;
                service.UpdateProduct(product1);
                service.GetAllProducts();

                // delete
                Console.WriteLine("\n=== DELETE Operation ===");
                service.DeleteProduct(2);
                service.GetAllProducts();

                Console.WriteLine("\n=== CRUD Operations Completed Successfully ===");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
            }

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}