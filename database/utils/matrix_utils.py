import pyarrow.parquet as pq
import pandas as pd

from database.utils import path_utils


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


def parse_bulk_meta_matrix(meta_df):
    result = []

    def process_value(value, is_numeric=False):
        if pd.isna(value):
            return None if is_numeric else ''
        return value

    for index, row in meta_df.iterrows():
        # 构建字典，键对应模型字段，值对应 DataFrame 中的值
        sample_data = {
            'sample_id': process_value(row['sample_id'], is_numeric=False),
            'dataset_name': process_value(row['dataset_name'], is_numeric=False),
            'disease_type': process_value(row['c_disease_type'], is_numeric=False),
            'primary_site': process_value(row['c_primiary_site'], is_numeric=False),
            'tumor_stage': process_value(row['c_tumor_stage'], is_numeric=False),
            'tumor_grade': process_value(row['c_tumor_grade'], is_numeric=False),
            'ethnicity': process_value(row['c_ethinicity'], is_numeric=False),
            'race': process_value(row['c_race'], is_numeric=False),
            'gender': process_value(row['c_gender'], is_numeric=False),
            'age': process_value(row['n_age'], is_numeric=True),
            'pfs': process_value(row['n_pfs'], is_numeric=True),
            'days_to_death': process_value(row['n_os'], is_numeric=True),
            'pfs_status': process_value(row['c_pfs_status'], is_numeric=False),
            'vital_status': process_value(row['c_os_status'], is_numeric=False)
        }

        # 将每行数据字典添加到结果列表中
        result.append(sample_data)

    return result


def parse_meta_matrix(dataset):
    meta_matrix_path = path_utils.get_dataset_samples_path(dataset)

    try:
        meta_df = pd.read_csv(meta_matrix_path)
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Meta matrix file for dataset '{dataset}' not found."
        ) from e

    if dataset.modality == 'bulkDNA':
        return parse_bulk_meta_matrix(meta_df)
