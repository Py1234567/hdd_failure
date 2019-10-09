import os

from codes.utils import create_spark_session
from codes.utils import unionAll


# Edited to take only one argument, so that it can be mapped to the list of folders
def read_csv_folder_spark(file_path):
    spark = create_spark_session("local[*]")
    df = (
        spark.read.format("com.databricks.spark.csv")
            .options(header="true", inferSchema="true")
            .load(file_path)
    )
    return df


spark = create_spark_session("local[*]")
main_dir = "backblaze"
# Example of structure of backblaze folder
"""
backblaze
├── data_Q2_2019
│   ├── 2019-04-01.csv
│   ├── 2019-04-02.csv
"""
# Remove any hidden folders
backblaze = [os.path.join(main_dir, x) for x in os.listdir(main_dir) if '.' not in x]

dfs = list(map(read_csv_folder_spark, backblaze))

final_df = unionAll(dfs)

final_df.coalesce(1).write.format('com.databricks.spark.csv').option("header", "true").save("final_data/hdd_data.csv")
# Write another compressed file for quick read
final_df.write.parquet.save("final_data/hdd_data.parquet")
