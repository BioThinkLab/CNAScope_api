import pandas as pd
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'CNAScope_api.settings')

django.setup()

from database.models import Dataset, BulkSampleMetadata
from django.db import transaction


def import_bulk_sample_metadata(dirpath):
    BulkSampleMetadata.objects.all().delete()

    for filename in os.listdir(dirpath):
        file_path = os.path.join(dirpath, filename)

        meta_df = pd.read_csv(file_path)

        dataset_name = meta_df.loc[0, 'dataset_name']
        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            print(f"Dataset {dataset_name} not found.")
            continue

        with transaction.atomic():
            metas = []
            for _, row in meta_df.iterrows():
                # 检查 sample_id 是否已存在，避免重复插入
                sample_id = row['sample_id']

                # 清洗数据，将 NaN 转换为 None 或合适的默认值
                disease_type = row.get('c_disease_type', '') if not pd.isna(row.get('c_disease_type')) else ''
                primary_site = row.get('c_primiary_site', '') if not pd.isna(row.get('c_primiary_site')) else ''
                tumor_stage = row.get('c_tumor_stage', '') if not pd.isna(row.get('c_tumor_stage')) else ''
                tumor_grade = row.get('c_tumor_grade', '') if not pd.isna(row.get('c_tumor_grade')) else ''
                ethnicity = row.get('c_ethinicity', '') if not pd.isna(row.get('c_ethinicity')) else ''
                race = row.get('c_race', '') if not pd.isna(row.get('c_race')) else ''
                gender = row.get('c_gender', '') if not pd.isna(row.get('c_gender')) else ''

                # 对数值字段做 NaN 处理，替换为 None
                age = row.get('n_age', None) if not pd.isna(row.get('n_age')) else None
                pfs = row.get('n_pfs', None) if not pd.isna(row.get('n_pfs')) else None
                days_to_death = row.get('n_os', None) if not pd.isna(row.get('n_os')) else None

                pfs_status = row.get('c_pfs_status', '') if not pd.isna(row.get('c_pfs_status')) else ''
                vital_status = row.get('c_os_status', '') if not pd.isna(row.get('c_os_status')) else ''

                # 构建一个新的 BulkSampleMetadata 实例
                metadata = BulkSampleMetadata(
                    sample_id=sample_id,
                    dataset=dataset,
                    disease_type=disease_type,
                    primary_site=primary_site,
                    tumor_stage=tumor_stage,
                    tumor_grade=tumor_grade,
                    ethnicity=ethnicity,
                    race=race,
                    gender=gender,
                    age=age,
                    pfs=pfs,
                    days_to_death=days_to_death,
                    pfs_status=pfs_status,
                    vital_status=vital_status
                )

                # 将当前记录加入待保存的 list
                metas.append(metadata)

            # 批量保存所有记录
            if metas:
                BulkSampleMetadata.objects.bulk_create(metas, batch_size=1000)
                print(f"Successfully imported {len(metas)} samples from {filename}")


if __name__ == '__main__':
    meta_dir = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\Meta\\bulk_case_meta'

    import_bulk_sample_metadata(meta_dir)
