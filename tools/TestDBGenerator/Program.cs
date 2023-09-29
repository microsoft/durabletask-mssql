// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TestDBGenerator;

using System.IO.Compression;
using DurableTask.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

static class Program
{
    static async Task Main(string[] args)
    {
        SqlConnectionStringBuilder builder = new(GetConnectionString());
        Server dbServer = new(new ServerConnection(new SqlConnection(builder.ToString())));

        // There's an assumption that the database schema version matches the assembly version.
        Version assemblyVersion = typeof(SqlOrchestrationService).Assembly.GetName().Version!;
        Version schemaVersion = new(assemblyVersion.Major, assemblyVersion.Minor, assemblyVersion.Build);

        string dbName = $"DurableDB-v{schemaVersion}";

        Database db = new(dbServer, dbName);
        db.Create();

        builder.InitialCatalog = db.Name;

        SqlOrchestrationServiceSettings settings = new(builder.ToString());
        SqlOrchestrationService service = new(settings);
        await service.CreateAsync();

        Console.WriteLine($"Created database '{db.Name}'.");

        Console.WriteLine($"Generating runtime data...");
        await Orchestrations.GenerateRuntimeDataAsync(service);

        string backupLocation = Path.Join(Environment.CurrentDirectory, $"{db.Name}.bak");
        Backup backup = new()
        {
            Database = db.Name,
            Action = BackupActionType.Database,
            CopyOnly = true,
            Incremental = false,
            SkipTapeHeader = true,
            UnloadTapeAfter = false,
            NoRewind = true,
            FormatMedia = true,
            Initialize = true,
            Devices =
            {
                new BackupDeviceItem(backupLocation, DeviceType.File),
            },
        };

        Console.WriteLine($"Backing up database to disk...");
        backup.SqlBackup(dbServer);

        Console.WriteLine($"Created backup file '{backupLocation}'.");

        // Drop the original database so that it can be restored
        db.UserAccess = DatabaseUserAccess.Restricted;
        db.Alter(TerminationClause.RollbackTransactionsImmediately);
        db.Refresh();
        db.Drop();

        // Restore
        Restore restore = new()
        {
            Database = $"{db.Name}-restored",
            Devices =
            {
                new BackupDeviceItem(backupLocation, DeviceType.File),
            },
        };

        Console.WriteLine("Restoring database from file (for validation)...");
        restore.SqlRestore(dbServer);

        Console.WriteLine($"Restored backup as '{db.Name}-restored");

        Console.WriteLine("Compressing backup file...");

        // Save to a zip file, which produces around 90% compression
        string zipFilePath = backupLocation + ".zip";
        using (ZipArchive archive = ZipFile.Open(zipFilePath, ZipArchiveMode.Create))
        {
            archive.CreateEntryFromFile(backupLocation, Path.GetFileName(backupLocation));
        }

        Console.WriteLine("Generated zipped backup file: " + zipFilePath);

        // Delete the uncompressed file
        File.Delete(backupLocation);
    }

    static string GetConnectionString()
    {
        string? connectionString = Environment.GetEnvironmentVariable("SQLDB_Connection");
        if (!string.IsNullOrEmpty(connectionString))
        {
            return connectionString;
        }

        Console.Error.WriteLine("Specify the database connection string in the 'SQLDB_Connection' environment variable.");
        Environment.Exit(1);
        return string.Empty;
    }
}