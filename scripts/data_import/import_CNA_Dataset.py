import pandas as pd
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'CNAScope_api.settings')

django.setup()

from database.models import Dataset
from django.db import transaction


def import_CNA_Dataset(file_path):
    dataset_df = pd.read_excel(file_path, 'Dataset Metadata')

    column_mapping = {
        "xxx": "name",
        "dataset_full_name": "full_name",
        "dataset_link": "link",
        "c_source": "source",
        "c_programme": "programme",
        "c_modality": "modality",
        "c_obs_type": "obs_type",
        "c_protocol": "protocol",
        "c_platform": "platform",
        "c_workflow": "workflow",
        "c_cn_type": "cn_type",
        "c_reference": "reference",
        "c_cancer_type": "cancer_type",
        "c_cancer_type_full_name": "cancer_type_full_name",
        "n_sample_num": "sample_num",
        "n_cell_num": "cell_num",
        "n_spot_num": "spot_num",
    }

    missing_cols = [col for col in column_mapping.keys() if col not in dataset_df.columns]
    if missing_cols:
        raise ValueError(f"Excel 缺少必要列: {missing_cols}")

    datasets = []
    for _, row in dataset_df.iterrows():
        data = {}
        for excel_col, model_field in column_mapping.items():
            value = row[excel_col]

            # 数字字段处理
            if model_field in ["sample_num", "cell_num", "spot_num"]:
                try:
                    value = int(value) if pd.notnull(value) else None
                except (ValueError, TypeError):
                    value = None

            # 字符串字段处理
            else:
                value = str(value).strip() if pd.notnull(value) else ""

            data[model_field] = value

        datasets.append(Dataset(**data))

    # 数据库事务写入
    try:
        with transaction.atomic():
            Dataset.objects.bulk_create(datasets, batch_size=500)
    except Exception as e:
        raise RuntimeError(f"写入数据库失败: {e}")


if __name__ == '__main__':
    data_file_path = 'E:\\WebProject\\CNAScope\\Data\\CNAScope table.xlsx'

    import_CNA_Dataset(data_file_path)
