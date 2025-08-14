import pandas as pd
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'CNAScope_api.settings')

django.setup()

from database.models import Dataset
from django.db.models import Sum


if __name__ == '__main__':
    total_counts = Dataset.objects.aggregate(
        total_sample_num=Sum('sample_num'),
        total_cell_num=Sum('cell_num'),
        total_spot_num=Sum('spot_num')
    )

    # 输出结果
    print(f"Total Sample Num: {total_counts['total_sample_num']}")
    print(f"Total Cell Num: {total_counts['total_cell_num']}")
    print(f"Total Spot Num: {total_counts['total_spot_num']}")

    # 统计 modality 为 ['bulkDNAles', 'bulkDNA'] 的 cancer_type 的数量
    count_cancer_types = Dataset.objects.filter(
        modality__in=["bulkDNAles", "bulkDNA"]
    ).values('cancer_type').distinct().count()
    bulk_sample_num = Dataset.objects.filter(
        modality__in=["bulkDNAles", "bulkDNA"]
    ).aggregate(total_sample_num=Sum('sample_num'))

    # 输出统计结果
    print(f"Number of unique cancer_types: {count_cancer_types}")
    print(f"Number of bulk sample: {bulk_sample_num}")

    cell_cancer_types = Dataset.objects.filter(
        modality__in=['scDNA', 'scRNA']
    ).values('cancer_type').distinct().count()
    cell_num = Dataset.objects.filter(
        modality__in=['scDNA', 'scRNA']
    ).aggregate(total_sample_num=Sum('cell_num'))

    print(f"Number of unique cancer_types: {cell_cancer_types}")
    print(f"Number of cell num: {cell_num}")

    spot_cancer_types = Dataset.objects.filter(
        modality__in=['ST']
    ).values('cancer_type').distinct().count()
    ST_samples = Dataset.objects.filter(
        modality__in=['ST']
    ).aggregate(total_sample_num=Sum('sample_num'))

    print(f"Number of unique cancer_types: {spot_cancer_types}")
    print(f"Number of unique samples: {ST_samples}")
