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
    internal class ProductionCountriesWorker:BackgroundService
    {
        private readonly ILogger<ProductionCountriesWorker> _logger;
        private readonly string _connectionString;

            
        public ProductionCountriesWorker(
            ILogger<ProductionCountriesWorker> logger,
            IConfiguration configuration)
        {
            _logger = (ILogger<ProductionCountriesWorker>?)logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var productionCountries = ReadProductionCountriesFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_production_countries.csv");
                await BulkInsertProductionCountriesAsync(productionCountries);


                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }

        private List<ProductionCountry> ReadProductionCountriesFromCsv(string filePath)
        {
            var productionCountries = new List<ProductionCountry>();

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

                    var productionCountry = new ProductionCountry();

                    // Parse and set the properties here
                    if (!int.TryParse(fields[0], out int movieId))
                    {
                        _logger.LogError($"Invalid integer value for 'MovieId' at row {parser.LineNumber}");
                        continue;
                    }

                    productionCountry.MovieId = movieId;
                    productionCountry.ProductionCountriesId = fields[1];
                    productionCountry.Name = fields[2];

                    productionCountries.Add(productionCountry);
                }
            }

            return productionCountries;
        }
        private DataTable ConvertProductionCountriesToDataTable(List<ProductionCountry> productionCountries)
        {
            DataTable table = new DataTable();

            // Define the columns
            table.Columns.Add("MovieId", typeof(int));
            table.Columns.Add("ProductionCountriesId", typeof(string));
            table.Columns.Add("Name", typeof(string));

            // Populate the DataTable from the list
            foreach (var productionCountry in productionCountries)
            {
                table.Rows.Add(productionCountry.MovieId, productionCountry.ProductionCountriesId, productionCountry.Name);
            }

            return table;
        }

        private async Task BulkInsertProductionCountriesAsync(List<ProductionCountry> productionCountries)
        {
            DataTable productionCountriesTable = ConvertProductionCountriesToDataTable(productionCountries);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "ProductionCountries"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("ProductionCountriesId", "ProductionCountriesId");
                    bulkCopy.ColumnMappings.Add("Name", "Name");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(productionCountriesTable);
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
