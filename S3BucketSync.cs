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
    private static readonly int ListConcurrency = int.TryParse(Environment.GetEnvironmentVariable("LIST_CONCURRENCY"), out int listConcurrency) ? listConcurrency : 50;

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

            // List all objects in both source and destination buckets before starting copies
            Log("Listing all objects in source and destination buckets...");
            var semaphore = new SemaphoreSlim(ListConcurrency);
            var listTasks = new List<Task<(string BucketName, List<S3Object> Objects)>>();

            for (int i = 0; i < SourceBuckets.Length; i++)
            {
                string sourceBucketName = SourceBuckets[i];
                string destinationBucketName = DestinationBuckets[i];

                await semaphore.WaitAsync();
                listTasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        List<S3Object> sourceObjects = await ListS3ObjectsWithCache(sourceS3Client, sourceBucketName);
                        List<S3Object> destinationObjects = await ListS3Objects(destS3Client, destinationBucketName);
                        return (sourceBucketName, sourceObjects);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }

            var bucketListings = await Task.WhenAll(listTasks);

            // Prepare program plan
            long totalFilesToCopy = 0;
            long totalSizeToCopy = 0;
            var appSizeSummary = new Dictionary<string, (long FileCount, long TotalSize)>();

            foreach (var (bucketName, sourceObjects) in bucketListings)
            {
                foreach (var obj in sourceObjects)
                {
                    totalFilesToCopy++;
                    totalSizeToCopy += obj.Size;

                    string appKey = ExtractAppKeyFromPath(obj.Key);
                    if (!appSizeSummary.ContainsKey(appKey))
                    {
                        appSizeSummary[appKey] = (0, 0);
                    }
                    appSizeSummary[appKey] = (appSizeSummary[appKey].FileCount + 1, appSizeSummary[appKey].TotalSize + obj.Size);
                }
            }

            // Print program plan
            Log("Program Plan:");
            Log($"Total files to copy: {totalFilesToCopy}");
            Log($"Total size to copy: {FormatFileSize(totalSizeToCopy)}");
            Log("Summary by Application:");
            foreach (var app in appSizeSummary)
            {
                Log($"{app.Key}: {app.Value.FileCount} files, {FormatFileSize(app.Value.TotalSize)}");
            }

            // Start the copying process
            for (DateTime date = startDate; date <= endDate; date = date.AddDays(1))
            {
                string datePrefix = date.ToString("yyyyMMdd");
                Log($"Processing date: {datePrefix}");

                for (int i = 0; i < SourceBuckets.Length; i++)
                {
                    string sourceBucketName = SourceBuckets[i];
                    string destinationBucketName = DestinationBuckets[i];
                    DateTime startTime = DateTime.Now;

                    try
                    {
                        // List objects in source bucket
                        List<S3Object> sourceObjects = await ListS3ObjectsWithCache(sourceS3Client, sourceBucketName, datePrefix);
                        long totalSourceSize = sourceObjects.Sum(obj => obj.Size);

                        // Filter objects by SourceApp if applicable
                        if (SourceApps.Length > 0)
                        {
                            sourceObjects = sourceObjects.Where(obj => SourceApps.Any(app => obj.Key.Contains($"/{app}/"))).ToList();
                        }

                        // List objects in destination bucket
                        List<S3Object> destinationObjects = await ListS3Objects(destS3Client, destinationBucketName, datePrefix);

                        // Compare directories and identify files to copy
                        var filesToCopy = CompareDirectories(sourceObjects, destinationObjects);
                        long remainingSize = filesToCopy.Sum(obj => obj.Size);
                        int totalFilesToCopyForDate = filesToCopy.Count;

                        Log($"Total files in source: {sourceObjects.Count}, Total size: {FormatFileSize(totalSourceSize)}");
                        Log($"Remaining files to copy: {totalFilesToCopyForDate}, Remaining size: {FormatFileSize(remainingSize)}");

                        // Copy files with limited concurrency
                        var copySemaphore = new SemaphoreSlim(MaxConcurrency);
                        var tasks = new List<Task>();
                        foreach (var file in filesToCopy)
                        {
                            await copySemaphore.WaitAsync();
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
                                catch (Exception ex)
                                {
                                    Log($"Error copying file {file.Key}: {ex.Message}");
                                }
                                finally
                                {
                                    copySemaphore.Release();
                                }
                            }));
                        }

                        await Task.WhenAll(tasks);

                        Log($"Completed copying for date: {datePrefix} in bucket: {sourceBucketName}");
                    }
                    catch (Exception ex)
                    {
                        Log($"Error processing bucket: {sourceBucketName} for date: {datePrefix}: {ex.Message}");
                    }
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
    private static async Task<List<S3Object>> ListS3Objects(AmazonS3Client s3Client, string bucketName, string datePrefix = null)
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

    // Lists objects in the given S3 bucket, checking for a cached file first
    private static async Task<List<S3Object>> ListS3ObjectsWithCache(AmazonS3Client s3Client, string bucketName, string datePrefix = null)
    {
        string cacheFileName = $"{bucketName}_{datePrefix}_listing.txt";
        if (File.Exists(cacheFileName))
        {
            return LoadDirectoryListing(cacheFileName);
        }
        return await ListS3Objects(s3Client, bucketName, datePrefix);
    }

    // Saves the directory listing of the objects in the bucket to a file
    private static void SaveDirectoryListing(string bucketName, string datePrefix, List<S3Object> objects)
    {
        string fileName = $"{bucketName}_{datePrefix}_listing.txt";
        using (var writer = new StreamWriter(fileName))
        {
            foreach (var obj in objects)
            {
                writer.WriteLine($"{obj.Key},{obj.LastModified:yyyy-MM-dd HH:mm:ss},{obj.Size}");
            }
        }
    }

    // Loads the directory listing from a file
    private static List<S3Object> LoadDirectoryListing(string fileName)
    {
        var objects = new List<S3Object>();
        using (var reader = new StreamReader(fileName))
        {
            string line;
            while ((line = reader.ReadLine()) != null)
            {
                var parts = line.Split(',');
                if (parts.Length >= 3 && long.TryParse(parts[2], out long size))
                {
                    objects.Add(new S3Object
                    {
                        Key = parts[0],
                        LastModified = DateTime.Parse(parts[1]),
                        Size = size
                    });
                }
            }
        }
        return objects;
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

    // Extracts the application key from the file path
    private static string ExtractAppKeyFromPath(string path)
    {
        var parts = path.Split('/');
        return parts.Length > 1 ? parts[1] : "unknown";
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
