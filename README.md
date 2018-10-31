# Github Plugin
This plugin moves data from the Github API to S3 based on the specified object.

## Hooks
### GithubHook
This hook handles the authentication and request to Github. This extends the HttpHook.

### S3Hook
Core Airflow S3Hook with the standard boto dependency.

## Operators
### GithubtoCloudStorageOperator
This operator composes the logic for this plugin. It fetches the Github specified object and saves the result in GCS. The parameters it can accept include the following:
```:param src: Path to the local file. (templated)
    :type src: str
    :param dst: Destination path within the specified bucket. (templated)
    :type dst: str
    :param bucket: The bucket to upload to. (templated)
    :type bucket: str
    :param google_cloud_storage_conn_id: The Airflow connection ID to upload with
    :type google_cloud_storage_conn_id: str
    :param mime_type: The mime-type string
    :type mime_type: str
    :param delegate_to: The account to impersonate, if any
    :type delegate_to: str
    :param gzip: Allows for file to be compressed and uploaded as gzip
```
