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
    internal class SpokenLanguagesWorker:BackgroundService
    {
        private readonly ILogger<SpokenLanguagesWorker> _logger;
        private readonly string _connectionString;
        public SpokenLanguagesWorker(
            ILogger<SpokenLanguagesWorker> logger,
            IConfiguration configuration)
        {
            _logger = (ILogger<SpokenLanguagesWorker>?)logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var lang = ReadSpokenLanguagesFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_spoken_languages.csv");
                await BulkInsertSpokenLanguagesAsync(lang);

                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }
        private List<SpokenLanguage> ReadSpokenLanguagesFromCsv(string filePath)
        {
            Helpers helpers = new Helpers();
            var spokenLanguages = new List<SpokenLanguage>();

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

                    var spokenLanguage = new SpokenLanguage();

                    // Parse and set the properties here
                    if (!helpers.TryParseInt(fields[0], out int movieId))
                    {
                        _logger.LogError($"Invalid float value for 'movieId' at row {parser.LineNumber}");
                        continue;
                    }
                    spokenLanguage.MovieId = movieId;

                    spokenLanguage.SpokenLanguagesId = fields[1];
                    spokenLanguage.Name = fields[2];

                    spokenLanguages.Add(spokenLanguage);
                }
            }

            return spokenLanguages;
        }
        private DataTable ConvertSpokenLanguagesToDataTable(List<SpokenLanguage> spokenLanguages)
        {
            DataTable table = new DataTable();

            // Define the columns
            table.Columns.Add("MovieId", typeof(int));
            table.Columns.Add("SpokenLanguagesId", typeof(string));
            table.Columns.Add("Name", typeof(string));

            // Populate the DataTable from the list
            foreach (var spokenLanguage in spokenLanguages)
            {
                table.Rows.Add(spokenLanguage.MovieId, spokenLanguage.SpokenLanguagesId, spokenLanguage.Name);
            }

            return table;
        }

        private async Task BulkInsertSpokenLanguagesAsync(List<SpokenLanguage> spokenLanguages)
        {
            DataTable spokenLanguagesTable = ConvertSpokenLanguagesToDataTable(spokenLanguages);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "SpokenLanguages"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("SpokenLanguagesId", "SpokenLanguagesId");
                    bulkCopy.ColumnMappings.Add("Name", "Name");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(spokenLanguagesTable);
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
