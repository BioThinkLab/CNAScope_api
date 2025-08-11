from rest_framework import serializers
from database.models import Dataset, BulkSampleMetadata


class DatasetSerializer(serializers.ModelSerializer):
    class Meta:
        model = Dataset
        exclude = ['created_at', 'updated_at']


class BulkSampleMetadataSerializer(serializers.ModelSerializer):
    class Meta:
        model = BulkSampleMetadata
        exclude = ['dataset', 'created_at', 'updated_at']
