import os
import csv

import pyarrow.parquet as pq
import pandas as pd

from django.http import HttpResponse

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from database.models import Dataset

from database.utils import path_utils


class CNAMatrixView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)

        if not dataset_name or not workflow_type:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        matrix_path = path_utils.get_dataset_matrix_path(dataset, workflow_type)

        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="matrix.csv"'

        try:
            with open(matrix_path, 'r', newline='') as file:
                reader = csv.reader(file)
                writer = csv.writer(response)
                for row in reader:
                    writer.writerow(row)
        except FileNotFoundError:
            return Response('CNA matrix file not found!', status=status.HTTP_404_NOT_FOUND)

        return response


class CNAMetaView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)

        if not dataset_name or not workflow_type:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        meta_path = path_utils.get_dataset_meta_path(dataset, workflow_type)

        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="meta.csv"'

        try:
            with open(meta_path, 'r', newline='') as file:
                reader = csv.reader(file)
                writer = csv.writer(response)
                for row in reader:
                    writer.writerow(row)
        except FileNotFoundError:
            return Response('CNA meta file not found!', status=status.HTTP_404_NOT_FOUND)

        return response


class CNATreeView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)

        if not dataset_name or not workflow_type:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        tree_path = path_utils.get_dataset_tree_path(dataset, workflow_type)

        try:
            with open(tree_path, 'r') as file:
                content = file.read()
        except FileNotFoundError:
            return Response('CNA tree file not found!', status=status.HTTP_404_NOT_FOUND)

        return Response(content)


class CNAGeneListView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)

        if not dataset_name or not workflow_type:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        gene_matrix_path = path_utils.get_dataset_gene_matrix_path(dataset, workflow_type)

        try:
            header = pq.read_schema(gene_matrix_path).names[1:]
        except FileNotFoundError:
            return Response('CNA gene matrix file not found!', status=status.HTTP_404_NOT_FOUND)

        data = [{"id": idx, "gene": gene} for idx, gene in enumerate(header)]

        return Response(data, status=status.HTTP_200_OK)


class CNANewickView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)

        if not dataset_name or not workflow_type:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        newick_path = path_utils.get_dataset_newick_path(dataset, workflow_type)

        try:
            with open(newick_path, 'r') as file:
                content = file.read()
        except FileNotFoundError:
            return Response('Newick file not found!', status=status.HTTP_404_NOT_FOUND)

        return Response(content)


class CNAGeneMatrixView(APIView):
    def post(self, request):
        dataset_name = request.data.get('datasetName')
        workflow_type = request.data.get('workflowType')
        genes = request.data.get('genes')

        # 基本参数校验
        if not dataset_name:
            return Response({'error': 'datasetName is required'}, status=status.HTTP_400_BAD_REQUEST)
        if not workflow_type:
            return Response({'error': 'workflowType is required'}, status=status.HTTP_400_BAD_REQUEST)
        if not isinstance(genes, list):
            return Response({'error': 'genes must be a list'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'error': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        gene_matrix_path = path_utils.get_dataset_gene_matrix_path(dataset, workflow_type)

        try:
            # 先读取 header（schema）
            schema = pq.read_schema(gene_matrix_path)
            all_columns = schema.names
            first_col = all_columns[0]

            # 构造需要的列：第一列 + 用户指定的存在的列
            selected_cols = [first_col] + [g for g in genes if g in all_columns and g != first_col]

            # 只读取需要的列（高效）
            df = pd.read_parquet(gene_matrix_path, engine='pyarrow', columns=selected_cols)

            # 转成 CSV 字符串
            csv_str = df.to_csv(index=False)

            # 返回 CSV
            resp = HttpResponse(csv_str, content_type='text/csv; charset=utf-8')
            resp['Content-Disposition'] = 'inline; filename="subset.csv"'

            return resp

        except FileNotFoundError:
            return Response({'error': 'Gene matrix file not found.'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



