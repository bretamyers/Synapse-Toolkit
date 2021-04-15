jsonFilePathSource = 'call-data/tmp1A74.json' #'JsonFiles/Activities/activities_02072019_103118_000.caliper.json.gz'
parquetFilePathTarget = 'call-data_flattened/tmp1A74' #'FlattenedJson/Activities/activities_02072019_103118_000.caliper.json.gz'
pipelineRunID = "789"
storageAccountContainerSource = 'teams-data' #'landingzone' 
storageAccountContainerTarget = 'teams-data' #'silverzone' 
storageAccountName = 'adlsbrmyers'
storageAccountCredential = 'UGAhJ3mcTUzADJt7byM4oHD+LDNf3yptmcXAXA+mue9U4x5zg1VXHmsSUrXfiSrrp8RH7d3Ii7IojVIaqnc+bQ=='


import json
from functools import (partial, singledispatch)
from itertools import chain
from typing import (Dict, List, TypeVar)

import datetime
import azure.storage.blob
import pandas as pd # NEED TO UPDATE THE DEFAULT pandas INSTALL WITH A MORE CURRENT VERSION 1.2.1 on the cluster
import io
import gzip

# https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python

Serializable = TypeVar('Serializable', None, int, bool, float, str, dict, list, tuple)
Array = List[Serializable]
Object = Dict[str, Serializable]


def flatten(object_: Object, *, path_separator: str = '_') -> Array[Object]:
    """
    Flattens given JSON object into list of objects with non-nested values.
    """
    keys = set(object_)
    result = [dict(object_)]
    while keys:
        key = keys.pop()
        new_result = []
        for index, record in enumerate(result):
            try:
                value = record[key]
            except KeyError:
                new_result.append(record)
            else:
                if isinstance(value, dict):
                    del record[key]
                    new_value = flatten_nested_objects(
                            value,
                            prefix=key + path_separator,
                            path_separator=path_separator)
                    keys.update(new_value.keys())
                    new_result.append({**new_value, **record})
                elif isinstance(value, list):
                    del record[key]
                    new_records = [
                        flatten_nested_objects(sub_value,
                                               prefix=key + path_separator,
                                               path_separator=path_separator)
                        for sub_value in value]
                    keys.update(chain.from_iterable(map(dict.keys,
                                                        new_records)))
                    new_result.extend({**new_record, **record}
                                      for new_record in new_records)
                else:
                    new_result.append(record)
        result = new_result
    return result


@singledispatch
def flatten_nested_objects(object_: Serializable, *, prefix: str = '', path_separator: str) -> Object:
    return {prefix[:-len(path_separator)]: object_}


@flatten_nested_objects.register(dict)
def _(object_: Object, *, prefix: str = '', path_separator: str) -> Object:
    result = dict(object_)
    for key in list(result):
        result.update(flatten_nested_objects(result.pop(key),
                                             prefix=(prefix + key + path_separator),
                                             path_separator=path_separator))
    return result


@flatten_nested_objects.register(list)
def _(object_: Array, *, prefix: str = '', path_separator: str) -> Object:
    return {prefix[:-len(path_separator)]: list(map(partial(
            flatten_nested_objects,
            path_separator=path_separator),
            object_))}



jsonFlattenedDatetime = datetime.datetime.now()

blob_service_client = azure.storage.blob.BlobServiceClient(account_url=f"https://{storageAccountName}.blob.core.windows.net", credential=storageAccountCredential)

# Check the file extension to see if its been gzipped
if blob_service_client.get_blob_client(container=storageAccountContainerSource, blob=jsonFilePathSource).get_blob_properties()['name'].split('/')[-1].split('.')[-1] == 'gz':
    # Read gzip compressed json
    fileText = gzip.decompress(blob_service_client.get_blob_client(container=storageAccountContainerSource, blob=jsonFilePathSource).download_blob().readall())
else:
    # Read uncompressed json
    fileText = blob_service_client.get_blob_client(container=storageAccountContainerSource, blob=jsonFilePathSource).download_blob().readall()


jsonObj = json.loads(f'{{"PipelineRunID": "{pipelineRunID}", "FlattenedDateTime": "{jsonFlattenedDatetime}", "Data": {fileText.decode("utf-8")}}}')
flat = flatten(jsonObj)
df = pd.json_normalize(flat)

output = io.BytesIO()
df.to_parquet(output)
blob_client = blob_service_client.get_blob_client(container=storageAccountContainerTarget, blob=f'{parquetFilePathTarget}_{jsonFlattenedDatetime.strftime("%Y%m%d_%H%M%S")}.parquet')
blob_client.upload_blob(output.getvalue())
    
    
    
