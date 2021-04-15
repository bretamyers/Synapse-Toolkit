# Flatten Json

A script that takes a source and target as parameters and competely flattens the json into a tabular format and outputs it into parquet file with the parsed datetiem appended to the file name. This handles any level of nested arrays within the json. 

**\*\*This needs to be used with caution and with a deep understanding of the data for downstream usage of the output. Since it will flatten all arrays in the file, it will explode the data n number of times.\*\***

The script expects the following parameters which can be passed in from a Synapse pipeline:
  1. The file path to the json file. The file can be raw json or a gzipped json file.
  2. The tartget file path for where the generate parquet file should be placed.
  3. The source storage account container.
  4. The target storage account container.
  5. The storage account name
  6. The storage account key. **TODO - Add Key Vault integration**
 
