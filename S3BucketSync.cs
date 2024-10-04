using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

class S3BucketSync
{
    // Initializes an Amazon S3 client using either provided credentials or default AWS credentials
    private static AmazonS3Client InitializeS3Client(string accessKey, string secretKey, string endpoint)
    {
        if (string.IsNullOrEmpty(accessKey) || string.IsNullOrEmpty(secretKey))
        {
            return new AmazonS3Client(new AmazonS3Config { ServiceURL = string.IsNullOrEmpty(endpoint) ? null : endpoint });
        }
        else
        {
            return new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), new AmazonS3Config { ServiceURL = string.IsNullOrEmpty(endpoint) ? null : endpoint });
        }
    }

    private static readonly string sourceEndpoint = Environment.GetEnvironmentVariable("SOURCE_ENDPOINT");
    private static readonly string destEndpoint = Environment.GetEnvironmentVariable("DEST_ENDPOINT");
    private static readonly string[] SourceBuckets = Environment.GetEnvironmentVariable("SOURCE_BUCKET_NAMES")?.Split(',') ?? new string[0];
    private static readonly string[] DestinationBuckets = Environment.GetEnvironmentVariable("DESTINATION_BUCKET_NAMES")?.Split(',') ?? new string[0];
    private static readonly string[] SourceApps = Environment.GetEnvironmentVariable("SOURCE_APPS")?.Split(',') ?? new string[0];
    private static readonly string FileCopyMode = Environment.GetEnvironmentVariable("FILE_COPY_MODE") ?? "memory"; // Options: "memory" or "disk"
    private static readonly string SourceAccessKey = Environment.GetEnvironmentVariable("SOURCE_ACCESS_KEY");
    private static readonly string SourceSecretKey = Environment.GetEnvironmentVariable("SOURCE_SECRET_KEY");
    private static readonly string DestAccessKey = Environment.GetEnvironmentVariable("DEST_ACCESS_KEY");
    private static readonly string DestSecretKey = Environment.GetEnvironmentVariable("DEST_SECRET_KEY");

    private static readonly DateTime DefaultStartDate = new DateTime(2023, 01, 01);
    private static readonly DateTime EndDate = DateTime.Now.AddDays(-1);

    private static readonly int MaxConcurrency = int.TryParse(Environment.GetEnvironmentVariable("MAX_CONCURRENCY"), out int concurrency) ? concurrency : 5;

    private static AmazonS3Client sourceS3Client;
    private static AmazonS3Client destS3Client;
    private static StreamWriter logWriter;

    static async Task Main(string[] args)
    {
        string logFileName = $"S3BucketSync_{DateTime.Now:yyyyMMdd_HHmmss}.log";
        logWriter = new StreamWriter(logFileName);

        try
        {
            // Initialize S3 clients with optional access keys or default credentials
            sourceS3Client = InitializeS3Client(SourceAccessKey, SourceSecretKey, sourceEndpoint);
            destS3Client = InitializeS3Client(DestAccessKey, DestSecretKey, destEndpoint);

            // Get date range from environment variables or use defaults
            DateTime startDate = GetStartDate();
            DateTime endDate = GetEndDate();

            // Loop over each date prefix
            for (DateTime date = startDate; date <= endDate; date = date.AddDays(1))
            {
                string datePrefix = date.ToString("yyyyMMdd");
                Log($"Processing date: {datePrefix}");

                foreach (var bucketName in SourceBuckets)
                {
                    DateTime startTime = DateTime.Now;

                    // List objects in source bucket
                    List<S3Object> sourceObjects = await ListS3Objects(sourceS3Client, bucketName, datePrefix);
                    long totalSourceSize = sourceObjects.Sum(obj => obj.Size);

                    // Filter objects by SourceApp if applicable
                    if (SourceApps.Length > 0)
                    {
                        sourceObjects = sourceObjects.Where(obj => SourceApps.Any(app => obj.Key.Contains($"/{app}/"))).ToList();
                    }

                    // List objects in destination bucket
                    string destinationBucketName = DestinationBuckets.FirstOrDefault() ?? bucketName;
                    List<S3Object> destinationObjects = await ListS3Objects(destS3Client, destinationBucketName, datePrefix);

                    // Compare directories and identify files to copy
                    var filesToCopy = CompareDirectories(sourceObjects, destinationObjects);
                    long remainingSize = filesToCopy.Sum(obj => obj.Size);
                    int totalFilesToCopy = filesToCopy.Count;

                    Log($"Total files in source: {sourceObjects.Count}, Total size: {FormatFileSize(totalSourceSize)}");
                    Log($"Remaining files to copy: {totalFilesToCopy}, Remaining size: {FormatFileSize(remainingSize)}");

                    // Copy files with limited concurrency
                    var semaphore = new SemaphoreSlim(MaxConcurrency);
                    var tasks = new List<Task>();
                    foreach (var file in filesToCopy)
                    {
                        await semaphore.WaitAsync();
                        tasks.Add(Task.Run(async () =>
                        {
                            try
                            {
                                if (FileCopyMode.Equals("memory", StringComparison.OrdinalIgnoreCase))
                                {
                                    await CopyFileInMemory(sourceS3Client, destS3Client, file, destinationBucketName);
                                }
                                else if (FileCopyMode.Equals("disk", StringComparison.OrdinalIgnoreCase))
                                {
                                    await CopyFileToDisk(sourceS3Client, destS3Client, file, destinationBucketName);
                                }
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        }));
                    }

                    int filesCopied = 0;
                    long copiedSize = 0;
                    System.Timers.Timer progressTimer = new System.Timers.Timer(1000);
                    progressTimer.Elapsed += (sender, e) =>
                    {
                        TimeSpan elapsedTime = DateTime.Now - startTime;
                        Console.Write($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] \rProgress: {filesCopied}/{totalFilesToCopy} files copied, {FormatFileSize(copiedSize)}/{FormatFileSize(remainingSize)} copied, Elapsed: {elapsedTime:hh\:mm\:ss}");
                    };
                    progressTimer.Start();

                    await Task.WhenAll(tasks);
                    filesCopied = filesToCopy.Count;
                    copiedSize = filesToCopy.Sum(file => file.Size);

                    progressTimer.Stop();
                    Console.Write($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] \rProgress: {filesCopied}/{totalFilesToCopy} files copied, {FormatFileSize(copiedSize)}/{FormatFileSize(remainingSize)} copied\n");

                    // Double-check directory listings to verify all files were copied
                    var updatedSourceObjects = await ListS3Objects(sourceS3Client, bucketName, datePrefix);
                    if (SourceApps.Length > 0)
                    {
                        updatedSourceObjects = updatedSourceObjects.Where(obj => SourceApps.Any(app => obj.Key.Contains($"/{app}/"))).ToList();
                    }
                    var updatedDestinationObjects = await ListS3Objects(destS3Client, destinationBucketName, datePrefix);

                    if (ValidateCopy(updatedSourceObjects, updatedDestinationObjects))
                    {
                        Log($"Successfully copied all files for date: {datePrefix} in bucket: {bucketName}");
                    }
                    else
                    {
                        Log($"Error: Mismatch in copied files for date: {datePrefix} in bucket: {bucketName}");
                    }

                    DateTime endTime = DateTime.Now;
                    TimeSpan duration = endTime - startTime;
                    Log($"Completed copying for date: {datePrefix} in bucket: {bucketName}. Duration: {duration}, Total files copied: {totalFilesToCopy}, Total size copied: {FormatFileSize(remainingSize)}");
                }
            }
        }
        finally
        {
            logWriter?.Dispose();
        }
    }

    // Retrieves the start date for the copy process from the environment or uses a default
    private static DateTime GetStartDate()
    {
        string startDateEnv = Environment.GetEnvironmentVariable("START_DATE");
        if (DateTime.TryParse(startDateEnv, out DateTime startDate))
        {
            return startDate;
        }
        return DefaultStartDate;
    }

    // Retrieves the end date for the copy process from the environment or uses a default
    private static DateTime GetEndDate()
    {
        string endDateEnv = Environment.GetEnvironmentVariable("END_DATE");
        if (DateTime.TryParse(endDateEnv, out DateTime endDate))
        {
            return endDate;
        }
        return EndDate;
    }

    // Lists objects in the given S3 bucket with a specific prefix (date)
    private static async Task<List<S3Object>> ListS3Objects(AmazonS3Client s3Client, string bucketName, string datePrefix)
    {
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = datePrefix
        };

        var response = await s3Client.ListObjectsV2Async(request);
        SaveDirectoryListing(bucketName, datePrefix, response.S3Objects);
        return response.S3Objects;
    }

    // Saves the directory listing of the objects in the bucket to a file
    private static void SaveDirectoryListing(string bucketName, string datePrefix, List<S3Object> objects)
    {
        string fileName = $"{bucketName}_{datePrefix}_listing.txt";
        using (var writer = new StreamWriter(fileName))
        {
            foreach (var obj in objects)
            {
                writer.WriteLine($"{obj.Key}, {obj.LastModified:yyyy-MM-dd HH:mm:ss}, {FormatFileSize(obj.Size)}");
            }
        }
    }

    // Compares source and destination objects and identifies which files need to be copied
    private static List<S3Object> CompareDirectories(List<S3Object> sourceObjects, List<S3Object> destinationObjects)
    {
        var destinationKeys = new HashSet<string>(destinationObjects.ConvertAll(obj => obj.Key));
        var filesToCopy = sourceObjects.FindAll(obj => !destinationKeys.Contains(obj.Key));
        return filesToCopy;
    }

    // Copies a file from the source bucket to the destination bucket using in-memory transfer
    private static async Task CopyFileInMemory(AmazonS3Client sourceClient, AmazonS3Client destinationClient, S3Object file, string destinationBucketName)
    {
        // Download file from source
        var getRequest = new GetObjectRequest
        {
            BucketName = file.BucketName,
            Key = file.Key
        };

        using (var response = await sourceClient.GetObjectAsync(getRequest))
        using (var memoryStream = new MemoryStream())
        {
            await response.ResponseStream.CopyToAsync(memoryStream);
            memoryStream.Seek(0, SeekOrigin.Begin);

            // Upload file to destination
            var putRequest = new PutObjectRequest
            {
                BucketName = destinationBucketName,
                Key = file.Key,
                InputStream = memoryStream
            };

            await destinationClient.PutObjectAsync(putRequest);
        }
    }

    // Copies a file from the source bucket to the destination bucket using disk as an intermediate storage
    private static async Task CopyFileToDisk(AmazonS3Client sourceClient, AmazonS3Client destinationClient, S3Object file, string destinationBucketName)
    {
        // Download file from source
        var getRequest = new GetObjectRequest
        {
            BucketName = file.BucketName,
            Key = file.Key
        };

        string tempFilePath = Path.GetTempFileName();
        try
        {
            using (var response = await sourceClient.GetObjectAsync(getRequest))
            using (var fileStream = File.Create(tempFilePath))
            {
                await response.ResponseStream.CopyToAsync(fileStream);
            }

            // Upload file to destination
            var putRequest = new PutObjectRequest
            {
                BucketName = destinationBucketName,
                Key = file.Key,
                FilePath = tempFilePath
            };

            await destinationClient.PutObjectAsync(putRequest);
        }
        finally
        {
            if (File.Exists(tempFilePath))
            {
                File.Delete(tempFilePath);
            }
        }
    }

    // Validates that all files were successfully copied by comparing source and destination listings
    private static bool ValidateCopy(List<S3Object> sourceObjects, List<S3Object> destinationObjects)
    {
        var destinationKeys = new HashSet<string>(destinationObjects.Select(obj => obj.Key));
        return sourceObjects.All(obj => destinationKeys.Contains(obj.Key));
    }

    // Formats file size in a human-readable format
    private static string FormatFileSize(long bytes)
    {
        string[] sizes = { "B", "KB", "MB", "GB", "TB" };
        double len = bytes;
        int order = 0;
        while (len >= 1024 && order < sizes.Length - 1)
        {
            order++;
            len /= 1024;
        }
        return $"{len:0.##} {sizes[order]}";
    }

    // Logs messages to both console and log file
    private static void Log(string message)
    {
        string logMessage = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
        Console.WriteLine(logMessage);
        logWriter.WriteLine(logMessage);
    }
}
