using Microsoft.Data.SqlClient;
using Microsoft.VisualBasic.FileIO;
using RecomendationSystemWorkerService.Models.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RecomendationSystemWorkerService
{
    internal class CreditsCrewWorker:BackgroundService
    {
        private readonly ILogger<CreditsCrewWorker> _logger;
        private readonly string _connectionString;
        


        public CreditsCrewWorker(
            ILogger<CreditsCrewWorker> logger,
            IConfiguration configuration)
        {
            _logger = (ILogger<CreditsCrewWorker>?)logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var creditsCrew = ReadCreditsCrewFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_credits_crew.csv");
                await BulkInsertCreditsCrewAsync(creditsCrew);


                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }
        private DataTable ConvertCreditsCrewToDataTable(List<CreditsCrew> creditsCrewList)
        {
            DataTable dataTable = new DataTable();

            dataTable.Columns.Add("CreditId", typeof(string));
            dataTable.Columns.Add("Department", typeof(string));
            dataTable.Columns.Add("Gender", typeof(int));
            dataTable.Columns.Add("Job", typeof(string));
            dataTable.Columns.Add("Name", typeof(string));
            dataTable.Columns.Add("ProfilePath", typeof(string));
            dataTable.Columns.Add("CrewId", typeof(int));
            dataTable.Columns.Add("MovieId", typeof(int));

            foreach (var crewMember in creditsCrewList)
            {
                var row = dataTable.NewRow();

                row["CreditId"] = crewMember.CreditId;
                row["Department"] = crewMember.Department;
                row["Gender"] = crewMember.Gender;
                row["Job"] = crewMember.Job;
                row["Name"] = crewMember.Name;
                row["ProfilePath"] = crewMember.ProfilePath;
                row["CrewId"] = crewMember.CrewId;
                row["MovieId"] = crewMember.MovieId;

                dataTable.Rows.Add(row);
            }

            return dataTable;
        }

        private async Task BulkInsertCreditsCrewAsync(List<CreditsCrew> creditsCrewList)
        {
            DataTable creditsCrewTable = ConvertCreditsCrewToDataTable(creditsCrewList);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "CreditsCrew"; // Set the destination table name

                    // Add column mappings to match the DataTable to the SQL Table
                    bulkCopy.ColumnMappings.Add("CreditId", "CreditId");
                    bulkCopy.ColumnMappings.Add("Department", "Department");
                    bulkCopy.ColumnMappings.Add("Gender", "Gender");
                    bulkCopy.ColumnMappings.Add("Job", "Job");
                    bulkCopy.ColumnMappings.Add("Name", "Name");
                    bulkCopy.ColumnMappings.Add("ProfilePath", "ProfilePath");
                    bulkCopy.ColumnMappings.Add("CrewId", "CrewId");
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(creditsCrewTable);
                        transaction.Commit(); // Commit the transaction if no exceptions
                        _logger.LogInformation("Bulk insert into CreditsCrew table completed successfully.");
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback(); // Rollback on error
                        _logger.LogError($"Bulk insert into CreditsCrew table failed: {ex.Message}", ex);
                    }
                }
            }
        }

        private List<CreditsCrew> ReadCreditsCrewFromCsv(string filePath)
        {
            var creditsCrewList = new List<CreditsCrew>();
            Helpers formatDatatypes = new Helpers();

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

                    var creditsCrew = new CreditsCrew();

                    creditsCrew.CreditId = fields[0];
                    creditsCrew.Department = fields[1];

                    if (!formatDatatypes.TryParseInt(fields[2], out int gender))
                    {
                        _logger.LogError($"Invalid int value for 'Gender' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    creditsCrew.Gender = gender;

                    creditsCrew.Job = fields[3];
                    creditsCrew.Name = fields[4];
                    creditsCrew.ProfilePath = fields[5];

                    if (!formatDatatypes.TryParseInt(fields[6], out int crewId))
                    {
                        _logger.LogError($"Invalid int value for 'CrewId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    creditsCrew.CrewId = crewId;

                    if (!formatDatatypes.TryParseInt(fields[7], out int movieId))
                    {
                        _logger.LogError($"Invalid int value for 'MovieId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    creditsCrew.MovieId = movieId;

                    creditsCrewList.Add(creditsCrew);
                }
            }

            return creditsCrewList;
        }

    }
}
