
import sys
sys.path.append('/home/kosala/git-repos/atlas-insights/')
from typing import List, Dict
from src.utils.common_util_func import load_metadata
from src.utils.generate_sql.generate_ddl_hive import gen_hive_table_ddl 


def create_hive_table_ddls(ocs_group_name: str) -> List[str]:
    # load metadata
    metadata = load_metadata(ocs_group_name)
    dataset_configs = metadata.get('dataset_config', [])
    
    for ds in dataset_configs:
        table_ddl = ""
        destination = ds.get('destination', {})
        ddl = gen_hive_table_ddl(ocs_group_name, destination)
        table_ddl += ddl
        
    # store ddls in a sql file
    ddl_file_path = f"sql/{ocs_group_name}_hive_ddl.sql"
    with open(ddl_file_path, 'w') as ddl_file:
        ddl_file.write(table_ddl)
        
    return [ddl_file_path]

if __name__ == "__main__":
    ocs_group = "ecommerce_transactions_fs"
    ddl_files = create_hive_table_ddls(ocs_group)
    print(f"Hive DDL files created: {ddl_files}")
    print("DDL Statements:")