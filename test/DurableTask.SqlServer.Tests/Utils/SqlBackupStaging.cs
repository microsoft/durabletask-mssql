// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#nullable enable
namespace DurableTask.SqlServer.Tests.Utils
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using Xunit.Abstractions;

    /// <summary>
    /// Makes a local database backup file readable by the SQL Server process. <c>RESTORE DATABASE</c>
    /// is executed by the server, so the file has to be on the server's own file system: it's copied
    /// into the <c>test/setup.ps1</c> container with <c>docker cp</c>, or used as-is when SQL Server
    /// is installed locally.
    /// </summary>
    static class SqlBackupStaging
    {
        // Names the container hosting SQL Server, or "none" to force the use of local paths.
        const string ContainerEnvVar = "DTFX_TEST_SQL_CONTAINER";

        // Matches the container name used by test/setup.ps1.
        const string DefaultContainerName = "mssql-server";

        const string ContainerBackupDir = "/var/opt/mssql/backup";

        static readonly TimeSpan CommandTimeout = TimeSpan.FromMinutes(2);

        static readonly Lazy<string?> ContainerName = new Lazy<string?>(ResolveContainerName);

        /// <summary>
        /// Gets whether SQL Server is in a docker container reachable via the docker CLI.
        /// </summary>
        public static bool IsServerContainerized => ContainerName.Value != null;

        /// <summary>
        /// Makes <paramref name="localFilePath"/> available to SQL Server and returns the path the
        /// server should read it from.
        /// </summary>
        public static string Stage(string localFilePath, ITestOutputHelper output)
        {
            string? containerName = ContainerName.Value;
            if (containerName == null)
            {
                // SQL Server is on this machine, so it can read the file directly.
                return localFilePath;
            }

            string fileName = Path.GetFileName(localFilePath);
            string serverFilePath = $"{ContainerBackupDir}/{fileName}";

            output.WriteLine($"Copying {localFilePath} to {containerName}:{serverFilePath}...");

            // /var/opt/mssql ownership varies between image builds, so create the directory as root
            // and hand it to 'mssql' (the image's default user, uid 10001).
            RunDocker(output, "exec", "-u", "0", containerName, "/bin/sh", "-c", $"mkdir -p {ContainerBackupDir} && chown mssql {ContainerBackupDir}");

            RunDocker(output, "cp", localFilePath, $"{containerName}:{serverFilePath}");

            // 'docker cp' preserves the source mode, which depends on the local umask.
            RunDocker(output, "exec", "-u", "0", containerName, "chmod", "0644", serverFilePath);

            return serverFilePath;
        }

        /// <summary>
        /// Deletes a file previously returned by <see cref="Stage"/>. Failures are ignored.
        /// </summary>
        public static void Unstage(string serverFilePath, ITestOutputHelper output)
        {
            string? containerName = ContainerName.Value;
            if (containerName == null)
            {
                // Nothing was staged - the caller owns the local file.
                return;
            }

            try
            {
                RunDocker(output, "exec", "-u", "0", containerName, "rm", "-f", serverFilePath);
            }
            catch (Exception e)
            {
                output.WriteLine($"Failed to delete {containerName}:{serverFilePath}: {e.Message}");
            }
        }

        static string? ResolveContainerName()
        {
            string? configuredName = Environment.GetEnvironmentVariable(ContainerEnvVar);
            if (!string.IsNullOrWhiteSpace(configuredName))
            {
                return string.Equals(configuredName, "none", StringComparison.OrdinalIgnoreCase) ? null : configuredName;
            }

            // Nothing configured, so look for the container created by test/setup.ps1.
            try
            {
                (int exitCode, string stdout, _) = RunDockerCore(
                    "inspect", "--format", "{{.State.Running}}", DefaultContainerName);

                if (exitCode == 0 && string.Equals(stdout.Trim(), "true", StringComparison.OrdinalIgnoreCase))
                {
                    return DefaultContainerName;
                }
            }
            catch (Exception)
            {
                // No usable docker CLI. Assume a local SQL Server instance.
            }

            return null;
        }

        static void RunDocker(ITestOutputHelper output, params string[] args)
        {
            (int exitCode, string stdout, string stderr) = RunDockerCore(args);
            if (exitCode != 0)
            {
                throw new InvalidOperationException(
                    $"'docker {string.Join(" ", args)}' failed with exit code {exitCode}.{Environment.NewLine}{stdout}{stderr}");
            }

            if (stdout.Length > 0)
            {
                output.WriteLine(stdout.TrimEnd());
            }
        }

        static (int ExitCode, string StdOut, string StdErr) RunDockerCore(params string[] args)
        {
            ProcessStartInfo startInfo = new()
            {
                FileName = "docker",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            foreach (string arg in args)
            {
                startInfo.ArgumentList.Add(arg);
            }

            using Process process = Process.Start(startInfo) ??
                throw new InvalidOperationException("Failed to start the docker CLI.");

            // Drain both streams concurrently so a full pipe buffer can't deadlock us.
            StringBuilder stdout = new();
            StringBuilder stderr = new();
            process.OutputDataReceived += (_, e) => { if (e.Data != null) stdout.AppendLine(e.Data); };
            process.ErrorDataReceived += (_, e) => { if (e.Data != null) stderr.AppendLine(e.Data); };
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            if (!process.WaitForExit((int)CommandTimeout.TotalMilliseconds))
            {
                try
                {
                    process.Kill();
                }
                catch (InvalidOperationException)
                {
                    // Exited between the timeout and the kill.
                }

                throw new TimeoutException($"'docker {string.Join(" ", args)}' did not complete within {CommandTimeout}.");
            }

            // Lets the async stream readers flush before the buffers are read.
            process.WaitForExit();

            return (process.ExitCode, stdout.ToString(), stderr.ToString());
        }
    }
}
