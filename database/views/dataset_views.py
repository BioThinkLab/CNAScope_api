import pandas as pd

from rest_framework import generics, status
from rest_framework.views import APIView
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework.response import Response

from database.models import Dataset, BulkSampleMetadata
from database.serializers.dataset_serializers import DatasetSerializer, BulkSampleMetadataSerializer
from database.utils import matrix_utils


class DatasetListView(ReadOnlyModelViewSet):
    queryset = Dataset.objects.all()
    serializer_class = DatasetSerializer
    pagination_class = None
    lookup_field = 'name'


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
            samples = matrix_utils.parse_meta_matrix(dataset, )
        except FileNotFoundError:
            return Response({"detail": "Meta matrix file not found."}, status=status.HTTP_404_NOT_FOUND)

        # 返回数据
        return Response(samples, status=status.HTTP_200_OK)
