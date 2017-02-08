# s3upload.py
# Can be used to upload large file to S3

#!/bin/python
import os
import sys
import argparse
import math

import boto
from boto.s3.key import Key
from filechunkio import FileChunkIO

def check_arg(args=None):
    parser = argparse.ArgumentParser(description='args : start/start, instance-id')
    parser.add_argument('-b', '--bucket',
                        help='bucket name',
                        required='True',
                        default='')
    parser.add_argument('-f', '--filename',
                        help='file to upload',
                        required='True',
                        default='')

    results = parser.parse_args(args)
    return (results.bucket,
            results.filename)


def upload_to_s3(file, bucket, key_name=None):
    source_size = 0
    source_path = file.name
    try:
        source_size = os.fstat(file.fileno()).st_size
    except:
        # Not all file objects implement fileno(),
        # so we fall back on this
        file.seek(0, os.SEEK_END)
        source_size = file.tell()

    print 'source_size=%s MB' %(source_size/(1024*1024))

    aws_access_key = boto.config.get('Credentials', 'aws_access_key_id')
    aws_secret_access_key = boto.config.get('Credentials', 'aws_secret_access_key')

    conn = boto.connect_s3(aws_access_key, aws_secret_access_key)

    bucket = conn.get_bucket(bucket, validate=True)
    print 'bucket=%s' %(bucket)

    # Create a multipart upload request
    mp = bucket.initiate_multipart_upload(os.path.basename(source_path) if key_name is None else key_name)

    # Use a chunk size of 50 MiB (feel free to change this)
    chunk_size = 52428800
    chunk_count = int(math.ceil(source_size / chunk_size))
    print 'chunk_count=%s' %(chunk_count)

    # Send the file parts, using FileChunkIO to create a file-like object
    # that points to a certain byte range within the original file. We
    # set bytes to never exceed the original file size.
    sent = 0
    for i in range(chunk_count + 1):
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        sent = sent +  bytes
        with FileChunkIO(source_path, 'r', offset=offset,
                         bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)
        print '%s: sent = %s MBytes ' %(i, sent/1024/1024)

    # Finish the upload
    mp.complete_upload()

    if sent == source_size:
        return True
    return False

if __name__ == '__main__':
    '''
    Usage:
    python s3upload.py -b s3-sample-bucket -f sample-file2
    '''

    bucket, filename = check_arg(sys.argv[1:])
    file = open(filename, 'r+')

    if upload_to_s3(file, bucket):
        print 'It works!'
    else:
        print 'The upload failed...'
