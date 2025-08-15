import pandas as pd

from django.db.models import Case, When, Value, IntegerField

from rest_framework import generics, status
from rest_framework.views import APIView
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework.response import Response

from database.models import Dataset, BulkSampleMetadata
from database.serializers.dataset_serializers import DatasetSerializer, BulkSampleMetadataSerializer
from database.utils import matrix_utils
import os

class DatasetListView(ReadOnlyModelViewSet):
    queryset = Dataset.objects.all()
    serializer_class = DatasetSerializer
    pagination_class = None
    lookup_field = 'name'

    def get_queryset(self):
        # 获取默认排序
        queryset = super().get_queryset()

        # 按照 status 的固定顺序排序
        queryset = queryset.annotate(
            source_order=Case(
                When(source='GDC Portal', then=Value(1)),
                When(source='cBioportal', then=Value(2)),
                When(source='archived', then=Value(3)),
                When(source='10x Official', then=Value(4)),
                When(source='HSCGD', then=Value(5)),
                When(source='scTML', then=Value(6)),
                default=Value(7),
                output_field=IntegerField()
            )
        ).order_by('source_order', 'name')  # 默认排序方式

        return queryset


class DatasetSampleListView(APIView):
    def get(self, request):
        # 从查询参数中获取 dataset_name
        dataset_name = request.query_params.get('dataset_name')

        # 如果没有传递 dataset_name 参数，返回错误提示
        if not dataset_name:
            return Response({"detail": "dataset_name parameter is required."}, status=status.HTTP_400_BAD_REQUEST)

        # 获取对应的 Dataset 对象
        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({"detail": "Dataset not found."}, status=status.HTTP_404_NOT_FOUND)

        try:
            samples = matrix_utils.parse_meta_matrix(dataset)
        except FileNotFoundError:
            return Response({"detail": "Meta matrix file not found."}, status=status.HTTP_404_NOT_FOUND)

        # 返回数据
        return Response(samples, status=status.HTTP_200_OK)

def download_dataset(request):
    dataset_name = request.query_params.get('dataset_name')
    if not dataset_name:
        return Response({"detail": "dataset_name parameter is required."}, status=status.HTTP_400_BAD_REQUEST)

    try:
        dataset = Dataset.objects.get(name=dataset_name)
    except Dataset.DoesNotExist:
        return Response({"detail": "Dataset not found."}, status=status.HTTP_404_NOT_FOUND)

    try:
        file_path = os.path.join('/mnt/cbc_adam/platform/CNAScope/data/download_zips', f'{dataset_name}.zip')
        with open(file_path, 'rb') as f:
            response = Response(f.read(), content_type='application/zip')
            response['Content-Disposition'] = f'attachment; filename="{dataset_name}.zip"'
            return response
    except FileNotFoundError:
        return Response({"detail": "Data file not found."}, status=status.HTTP_404_NOT_FOUND)