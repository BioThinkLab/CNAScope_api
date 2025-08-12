import os
import csv

import pyarrow.parquet as pq
import pandas as pd

from django.http import HttpResponse

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from database.models import Dataset

from database.utils import path_utils, matrix_utils


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

                # 读取第一行（列名）
                header = next(reader)
                # 将第一列名称更改为 'id'
                header[0] = 'id'
                writer.writerow(header)  # 写入新的列名

                # 逐行写入数据
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

                # 读取第一行（列名）
                header = next(reader)
                # 将第一列名称更改为 'id'
                header[0] = 'id'
                writer.writerow(header)  # 写入新的列名

                # 逐行写入数据
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
            # 提取 Gene 的 CNA 矩阵
            df = matrix_utils.extract_matrix_from_parquet(gene_matrix_path, genes)

            # 重命名索引为 'id'
            df.index.name = 'id'

            # 转成 CSV 字符串
            csv_str = df.to_csv()

            # 返回 CSV
            resp = HttpResponse(csv_str, content_type='text/csv; charset=utf-8')
            resp['Content-Disposition'] = 'inline; filename="subset.csv"'

            return resp

        except FileNotFoundError:
            return Response({'error': 'Gene matrix file not found.'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class CNATermListView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)

        if not dataset_name or not workflow_type:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        term_matrix_path = path_utils.get_dataset_term_matrix_path(dataset, workflow_type)

        try:
            header = pq.read_schema(term_matrix_path).names[1:]
        except FileNotFoundError:
            return Response('CNA gene matrix file not found!', status=status.HTTP_404_NOT_FOUND)

        data = [{"id": idx, "gene": gene} for idx, gene in enumerate(header)]

        return Response(data, status=status.HTTP_200_OK)


class CNATermMatrixView(APIView):
    def post(self, request):
        dataset_name = request.data.get('datasetName', None)
        workflow_type = request.data.get('workflowType', None)
        terms = request.data.get('terms', None)

        if not dataset_name or not workflow_type or not terms:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        if not isinstance(terms, list):
            return Response({'error': 'terms must be a list'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'error': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        term_matrix_path = path_utils.get_dataset_term_matrix_path(dataset, workflow_type)

        try:
            # 提取 Term 的 CNA 矩阵
            df = matrix_utils.extract_matrix_from_parquet(term_matrix_path, terms)

            # 重命名索引为 'id'
            df.index.name = 'id'

            # 转成 CSV 字符串
            csv_str = df.to_csv()

            # 返回 CSV
            resp = HttpResponse(csv_str, content_type='text/csv; charset=utf-8')
            resp['Content-Disposition'] = 'inline; filename="subset.csv"'

            return resp
        except FileNotFoundError:
            return Response({'error': 'Term matrix file not found.'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

