# S3 Bucket Sync Tool

This is a C# application for syncing files between on-prem NetApp S3-compatible storage and AWS S3 buckets. The tool iterates through a date-based folder structure to efficiently copy files from the source to the destination bucket, ensuring only missing files are transferred.

## Features

- **S3 to S3 File Sync**: Copies files from on-prem NetApp S3-compatible storage to AWS S3.
- **Date-Based Iteration**: Processes files in each bucket organized by date prefix (`YYYYMMDD`).
- **Concurrency Control**: Supports limited concurrency for efficient file transfers.
- **Two Copy Modes**: Supports in-memory transfers or disk-based temporary storage to reduce memory usage.
- **Logging**: Logs progress and file operations to both the console and a log file.
- **Resumable Sync**: Uses checkpoints to prevent duplicate copies and ensure progress is maintained.

## Prerequisites

- .NET Core SDK
- AWS credentials configured through environment variables or default credentials (AWS CLI)

## Configuration

The application can be configured using environment variables:

- `SOURCE_ENDPOINT`: Endpoint for the source S3-compatible storage.
- `DEST_ENDPOINT`: Endpoint for the AWS S3 destination.
- `SOURCE_BUCKET_NAMES`: Comma-separated list of source bucket names.
- `DESTINATION_BUCKET_NAMES`: Comma-separated list of destination bucket names.
- `SOURCE_APPS`: Comma-separated list of source app names to filter files.
- `FILE_COPY_MODE`: Copy mode (`memory` or `disk`). Default is `memory`.
- `SOURCE_ACCESS_KEY` / `SOURCE_SECRET_KEY`: Access credentials for the source S3 storage.
- `DEST_ACCESS_KEY` / `DEST_SECRET_KEY`: Access credentials for the destination S3 storage.
- `START_DATE` / `END_DATE`: Override default date range (`YYYY-MM-DD` format).
- `MAX_CONCURRENCY`: Maximum number of concurrent file transfers.

## Running the Application

1. Clone the repository and navigate to the project directory.

2. Set the required environment variables.

3. Run the application:

   ```sh
   dotnet run
   ```

   You can also pass configuration via command-line arguments if environment variables are not set.

## Example Usage

```sh
export SOURCE_ENDPOINT="https://netapp.example.com"
export DEST_ENDPOINT="https://s3.amazonaws.com"
export SOURCE_BUCKET_NAMES="bucket1,bucket2"
export DESTINATION_BUCKET_NAMES="bucket1-backup"
export FILE_COPY_MODE="memory"
export MAX_CONCURRENCY="3"

dotnet run
```

## Logging

The application creates a log file in the current directory with the format `S3BucketSync_YYYYMMDD_HHmmss.log`, recording details about file transfers, progress, and any errors.

## File Copy Modes

- **In-Memory**: Files are downloaded into memory before uploading to the destination. This is fast but uses more memory.
- **Disk-Based**: Files are downloaded to a temporary file on disk before being uploaded. This reduces memory usage at the cost of more disk I/O.

## Concurrency

The application supports concurrent file transfers to improve performance. The concurrency level can be configured via the `MAX_CONCURRENCY` environment variable.

## License

This project is licensed under the MIT License.

