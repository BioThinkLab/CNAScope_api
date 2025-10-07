import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'CNAScope_api.settings')

django.setup()

from database.models import Dataset
from django.db.models import Sum


if __name__ == '__main__':
    sc_rna_cna_profile = Dataset.objects.filter(modality='scRNA').aggregate(
        total_cell_num=Sum('cell_num'),
    )

    print(f"scRNA CNA Profiles: {sc_rna_cna_profile['total_cell_num']}")

    spa_rna_cna_profile = Dataset.objects.filter(modality='spaRNA').aggregate(
        total_cell_num=Sum('cell_num'),
        total_spot_num=Sum('spot_num')
    )

    print(f"spaRNA CNA Profiles: {spa_rna_cna_profile['total_cell_num'] + spa_rna_cna_profile['total_spot_num']}")
