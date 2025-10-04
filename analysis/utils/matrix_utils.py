import pyarrow.parquet as pq
import pandas as pd
import numpy as np

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


def extract_matrix_from_csv(file_path, target_column):
    # 先读取 CSV 文件的头部（schema）
    df = pd.read_csv(file_path, nrows=1)
    all_columns = df.columns.tolist()
    first_col = all_columns[0]

    # 构造需要的列：第一列 + 用户指定的存在的列
    selected_cols = [first_col] + [c for c in target_column if c in all_columns and c != first_col]

    # 只读取需要的列（高效）
    df = pd.read_csv(file_path, usecols=selected_cols, index_col=0)

    return df


def calculate_abundance(file_path):
    try:
        df = pd.read_csv(file_path)

        # 排除第一列（index 0），选择剩余的列
        matrix_values = df.iloc[:, 1:].to_numpy().flatten()  # 摊平从第二列开始的所有数据

        # 获取最小值和最大值
        min_value = matrix_values.min()
        max_value = matrix_values.max()

        # 以 0.1 为步长，创建从最小值到最大值的区间
        bins = np.arange(min_value, max_value + 0.1, 0.1)

        # 计算每个区间的丰度（出现频率）
        abundance, _ = np.histogram(matrix_values, bins=bins)

        # 计算区间的中点
        bin_centers = (bins[:-1] + bins[1:]) / 2  # 区间的中点

        # 标准化 bin_center 到 0.1
        bin_centers_normalized = np.round(bin_centers, 2)  # 保留到 0.01 位

        # 组织成 [bin_center, abundance] 的形式
        bin_abundance_list = [[bin_center, abundance[i]] for i, bin_center in enumerate(bin_centers_normalized)]

        # 在列表首尾分别添加第一个区间的左边界和最后一个区间的右边界
        bin_abundance_list.insert(0, [bins[0], 0])  # 添加第一个区间的左边界
        bin_abundance_list.append([bins[-1], 0])  # 添加最后一个区间的右边界

        return bin_abundance_list
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Matrix Not Found."
        ) from e


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


def parse_scDNA_10x_meta_matrix(meta_df):
    result = []

    def process_value(value, is_numeric=False):
        if pd.isna(value):
            return None if is_numeric else ''
        return value

    for index, row in meta_df.iterrows():
        sample_data = {
            'cell_id': process_value(row['cell_id'], is_numeric=False),
            'total_num_reads': process_value(row['c_total_num_reads'], is_numeric=True),
            'num_unmapped_reads': process_value(row['c_num_unmapped_reads'], is_numeric=True),
            'num_lowmapq_reads': process_value(row['c_num_lowmapq_reads'], is_numeric=True),
            'num_duplicate_reads': process_value(row['c_num_duplicate_reads'], is_numeric=True),
            'num_mapped_dedup_reads': process_value(row['c_num_mapped_dedup_reads'], is_numeric=True),
            'frac_mapped_duplicates': process_value(row['c_frac_mapped_duplicates'], is_numeric=True),
            'effective_depth_of_coverage': process_value(row['c_effective_depth_of_coverage'], is_numeric=True),
            'effective_reads_per_1Mbp': process_value(row['c_effective_reads_per_1Mbp'], is_numeric=True),
            'raw_mapd': process_value(row['c_raw_mapd'], is_numeric=True),
            'normalized_mapd': process_value(row['c_normalized_mapd'], is_numeric=True),
            'raw_dimapd': process_value(row['c_raw_dimapd'], is_numeric=True),
            'normalized_dimapd': process_value(row['c_normalized_dimapd'], is_numeric=True),
            'mean_ploidy': process_value(row['c_mean_ploidy'], is_numeric=True),
            'ploidy_confidence': process_value(row['c_ploidy_confidence'], is_numeric=True),
            'is_high_dimapd': process_value(row['n_is_high_dimapd'], is_numeric=True),
            'is_noisy': process_value(row['n_is_noisy'], is_numeric=True),
            'est_cnv_resolution_mb': process_value(row['c_est_cnv_resolution_mb'], is_numeric=True),
        }

        # 将每行数据字典添加到结果列表中
        result.append(sample_data)

    return result


def parse_single_cell_meta_matrix(meta_df):
    result = []

    def process_value(value, is_numeric=False):
        if pd.isna(value):
            return None if is_numeric else ''
        return value

    for index, row in meta_df.iterrows():
        # 构建字典，键对应模型字段，值对应 DataFrame 中的值
        sample_data = {
            'cell_id': process_value(row['cell_id'], is_numeric=False),
            'dataset_name': process_value(row['dataset_name'], is_numeric=False),
            'cell_type': process_value(row['c_cell_type'], is_numeric=False),
            'confidence': process_value(row['c_confidence'], is_numeric=False),
            'donor': process_value(row['c_donor'], is_numeric=False),
            'cnv_score': process_value(row['n_cnv_score'], is_numeric=True),
            'cnv_status': process_value(row['c_cnv_status'], is_numeric=False),
            'malignancy': process_value(row['c_malignancy'], is_numeric=False),
            'cell_label': process_value(row['c_cell_label'], is_numeric=False),
        }

        # 将每行数据字典添加到结果列表中
        result.append(sample_data)

    return result


def parse_ST_meta_matrix(meta_df):
    result = []

    def process_value(value, is_numeric=False):
        if pd.isna(value):
            return None if is_numeric else ''
        return value

    for index, row in meta_df.iterrows():
        # 构建字典，键对应模型字段，值对应 DataFrame 中的值
        sample_data = {
            'spot_id': process_value(row['spot_id'], is_numeric=False),
            'dataset_name': process_value(row['dataset_name'], is_numeric=False),
            'cell_type': process_value(row['c_cell_type'], is_numeric=False),
            'confidence': process_value(row['c_confidence'], is_numeric=False),
            'donor': process_value(row['c_donor'], is_numeric=False),
            'cnv_score': process_value(row['n_cnv_score'], is_numeric=True),
            'cnv_status': process_value(row['c_cnv_status'], is_numeric=False),
            'malignancy': process_value(row['c_malignancy'], is_numeric=False),
            'cell_label': process_value(row['c_cell_label'], is_numeric=False),
            'spatial_1': process_value(row['n_spatial_1'], is_numeric=True),
            'spatial_2': process_value(row['n_spatial_2'], is_numeric=True)
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
    elif dataset.modality == 'scDNA':
        if dataset.source == '10x Official':
            return parse_scDNA_10x_meta_matrix(meta_df)
        else:
            return parse_single_cell_meta_matrix(meta_df)
    elif dataset.modality == 'scRNA':
        return parse_single_cell_meta_matrix(meta_df)
    else:
        return parse_ST_meta_matrix(meta_df)
