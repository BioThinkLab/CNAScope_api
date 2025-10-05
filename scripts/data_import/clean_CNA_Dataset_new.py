import pandas as pd
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'CNAScope_api.settings')

django.setup()

from database.models import Dataset


if __name__ == '__main__':
    Dataset.objects.all().delete()
