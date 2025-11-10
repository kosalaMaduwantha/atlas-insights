from typing import List, Dict

HIVE_TYPE_MAP = {
    'string': 'STRING',
    'integer': 'INT',
    'int':'INT',
    'datetime': 'TIMESTAMP',
    'date': 'DATE',
    'float': 'DECIMAL',
}

FILE_FORMAT_MAP = {
    'parquet': 'PARQUET',
    'orc': 'ORC',
}

COMPRESSION_MAP = {
    'parquet': 'parquet.compression',
    'orc': 'orc.compress',
}

def gen_hive_table_ddl(
    ocs_group_name: str, 
    destination_metadata: Dict,
    file_format: str
) -> str:

    table_name = destination_metadata.get('name')
    features = destination_metadata.get('features', [])
    path = destination_metadata.get('path', '')

    # Create Hive DDL statement
    ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS {ocs_group_name}_{table_name} (\n"
    for feature in features:
        ddl += f"    {feature.get('name')} {HIVE_TYPE_MAP.get(feature.get('dtype'), 'STRING')},\n"
    ddl = ddl.rstrip(",\n") + "\n) \n"
    ddl += f"STORED AS {FILE_FORMAT_MAP.get(file_format, 'PARQUET')}\n"
    ddl += f"LOCATION '{path}'\n"
    ddl += f"TBLPROPERTIES ('{COMPRESSION_MAP.get(file_format, 'parquet.compression')}'='SNAPPY'); \n\n"
    
    return ddl