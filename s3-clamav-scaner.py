import boto3
import clamd
import os


c=0

def make_tags(tags):
    tag_list = []
    for k,v in tags.items():
        tag_list.append({'Key':k,
                         'Value':v if v is not None else ''})

    return {'TagSet':tag_list}


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    global c
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
	    c=c+1
	    print(c)    
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                 yield "File:" +key
		 response = s3.get_object_tagging(
	    		Bucket=bucket,
            		Key=key,
                      )
    		 tag_set = response.get("TagSet")
		 for tt in tag_set:
		     if tt['Key']=="clamav-status":
	                      print("file tagged");
	             else:
		           print("start download")
			   s3.download_file(bucket,key,"/tmp/" + key)
	        	   print("try connect to clamav")
	                   cd = clamd.ClamdUnixSocket("/var/run/clamd.scan/clamd.sock")
	                   print("Scan file on clamav")	           
	                   scanresp = cd.scan("/tmp/" + key)
	                   ss=list(scanresp.values())
	                   if (ss[0][0]=="OK"):
	        	     print("File is ok")
	        	     print("Delete file from /tmp/ ")
	        	     os.remove("/tmp/" + key)
	        	     print("Try to make a tag")
	        	     tagset = make_tags({'clamav-status':'clean'})
	        	     s3.put_object_tagging(Bucket=bucket, Key=key,
	        	                                           Tagging=tagset)
	        	   else:
	        	     print("File is infected")
	        	     print("Try to mark as infected")
	        	     tagset = make_tags({'clamav-status':'infected'})
	        	     s3.put_object_tagging(Bucket=bucket, Key=key,
	        	                                           Tagging=tagset)	        	     
	        	
	         print("--------")
        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

for key in get_matching_s3_keys(bucket='S3-bucket-name-for-scan', prefix='', suffix=''):
    print(key)
    