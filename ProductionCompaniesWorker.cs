using Microsoft.Data.SqlClient;
using Microsoft.VisualBasic.FileIO;
using RecomendationSystemWorkerService.Models.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RecomendationSystemWorkerService
{
    internal class ProductionCompaniesWorker:BackgroundService
    {
        private readonly ILogger<ProductionCompaniesWorker> _logger;
        private readonly string _connectionString;


        public ProductionCompaniesWorker(
            ILogger<ProductionCompaniesWorker> logger,
            IConfiguration configuration)
        {
            _logger = (ILogger<ProductionCompaniesWorker>?)logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {


                var productionCampanies = ReadProductionCompaniesFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_production_companies.csv");
                await BulkInsertProductionCompaniesAsync(productionCampanies);

                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }

        private List<ProductionCompany> ReadProductionCompaniesFromCsv(string filePath)
        {
            var productionCompanies = new List<ProductionCompany>();

            using (TextFieldParser parser = new TextFieldParser(filePath))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                parser.HasFieldsEnclosedInQuotes = true;

                // Skip header row
                if (!parser.EndOfData) parser.ReadLine();

                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();

                    var productionCompany = new ProductionCompany();

                    // Parse and set the properties here
                    if (!int.TryParse(fields[0], out int movieId))
                    {
                        // Handle parsing error
                        continue;
                    }
                    productionCompany.MovieId = movieId;

                    if (!int.TryParse(fields[1], out int productionCompanyId))
                    {
                        // Handle parsing error
                        continue;
                    }
                    productionCompany.ProductionCompaniesId = productionCompanyId;

                    productionCompany.Name = fields[2];

                    productionCompanies.Add(productionCompany);
                }
            }

            return productionCompanies;
        }
        private DataTable ConvertProductionCompaniesToDataTable(List<ProductionCompany> productionCompanies)
        {
            DataTable table = new DataTable();

            // Define the columns
            table.Columns.Add("MovieId", typeof(int));
            table.Columns.Add("ProductionCompaniesId", typeof(int));
            table.Columns.Add("Name", typeof(string));

            // Populate the DataTable from the list
            foreach (var productionCompany in productionCompanies)
            {
                table.Rows.Add(productionCompany.MovieId, productionCompany.ProductionCompaniesId, productionCompany.Name);
            }

            return table;
        }

        private async Task BulkInsertProductionCompaniesAsync(List<ProductionCompany> productionCompanies)
        {
            DataTable productionCompaniesTable = ConvertProductionCompaniesToDataTable(productionCompanies);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "ProductionCompanies"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("ProductionCompaniesId", "ProductionCompaniesId");
                    bulkCopy.ColumnMappings.Add("Name", "Name");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(productionCompaniesTable);
                        transaction.Commit(); // Commit the transaction if no exceptions
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback(); // Rollback on error
                        _logger.LogError($"Bulk insert failed: {ex.Message}", ex);
                    }
                }
            }
        }

    }
}
