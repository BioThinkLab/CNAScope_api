import pyarrow.parquet as pq
import pandas as pd


def extract_matrix_from_parquet(file_path, target_column):
    # 先读取 header（schema）
    schema = pq.read_schema(file_path)
    all_columns = schema.names
    first_col = all_columns[0]

    # 构造需要的列：第一列 + 用户指定的存在的列
    selected_cols = [first_col] + [c for c in target_column if c in all_columns and c != first_col]

    # 只读取需要的列（高效）
    df = pd.read_parquet(file_path, engine='pyarrow', columns=selected_cols)

    return df
