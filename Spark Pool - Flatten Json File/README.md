# Flatten Json

A script that takes a source and target as parameters and competely flattens the json into a tabular format and outputs it into parquet. This handles any level of nested arrays within the json. 

**\*\*This needs to be used with caution and with a deep understanding of the data for downstream usage of the output. Since it will flatten all arrays in the file, it will explode the data n number of times.\*\***
