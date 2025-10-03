import os
import csv
import json

import pyarrow.parquet as pq
import pandas as pd

from django.http import HttpResponse

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from .models import *
from analysis.utils import path_utils, matrix_utils, recurrent_utils


class CNAMatrixView(APIView):
    def get(self, request):
        task_uuid = request.query_params.get('uuid', None)
        
        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        matrix_path = task.get_input_file_absolute_path()

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
        task_uuid = request.query_params.get('uuid', None)
        
        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        output_dir = task.get_output_dir_absolute_path()
        meta_path = os.path.join(output_dir, f'{task_uuid}_meta_scsvas.csv')

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
        task_uuid = request.query_params.get('uuid', None)
        
        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        output_dir = task.get_output_dir_absolute_path()
        tree_path = os.path.join(output_dir, f'{task_uuid}_cut50.json')

        try:
            with open(tree_path, 'r') as file:
                content = file.read()
        except FileNotFoundError:
            return Response('CNA tree file not found!', status=status.HTTP_404_NOT_FOUND)

        return Response(content)


class CNAGeneListView(APIView):
    def get(self, request):
        task_uuid = request.query_params.get('uuid', None)
        
        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        output_dir = task.get_output_dir_absolute_path()
        gene_matrix_path = os.path.join(output_dir, f'{task_uuid}_gene_cna.parquet')

        try:
            header = pq.read_schema(gene_matrix_path).names[1:]
        except FileNotFoundError:
            return Response('CNA gene matrix file not found!', status=status.HTTP_404_NOT_FOUND)

        data = [{"id": idx, "gene": gene} for idx, gene in enumerate(header)]

        return Response(data, status=status.HTTP_200_OK)


class CNANewickView(APIView):
    def get(self, request):
        task_uuid = request.query_params.get('uuid', None)
        
        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        output_dir = task.get_output_dir_absolute_path()
        newick_path = os.path.join(output_dir, f'{task_uuid}.nwk')

        try:
            with open(newick_path, 'r') as file:
                content = file.read()
        except FileNotFoundError:
            return Response('Newick file not found!', status=status.HTTP_404_NOT_FOUND)

        return Response(content)


class CNAGeneMatrixView(APIView):
    def post(self, request):
        task_uuid = request.data.get('uuid', None)
        genes = request.data.get('genes')

        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        output_dir = task.get_output_dir_absolute_path()
        gene_matrix_path = os.path.join(output_dir, f'{task_uuid}_gene_cna.parquet')

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
        task_uuid = request.query_params.get('uuid', None)
        
        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        output_dir = task.get_output_dir_absolute_path()
        term_matrix_path = os.path.join(output_dir, f'{task_uuid}_term_cna.parquet')

        try:
            header = pq.read_schema(term_matrix_path).names[1:]
        except FileNotFoundError:
            return Response('CNA gene matrix file not found!', status=status.HTTP_404_NOT_FOUND)

        data = [{"id": idx, "gene": gene} for idx, gene in enumerate(header)]

        return Response(data, status=status.HTTP_200_OK)


class CNATermMatrixView(APIView):
    def post(self, request):
        terms = request.data.get('terms', None)
        task_uuid = request.data.get('uuid', None)
        
        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        output_dir = task.get_output_dir_absolute_path()
        term_matrix_path = os.path.join(output_dir, f'{task_uuid}_term_cna.parquet')

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


class FocalCNAInfoView(APIView):
    def get(self, request):
        task_uuid = request.query_params.get('uuid', None)
        
        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        output_dir = task.get_output_dir_absolute_path()

        files = os.listdir(output_dir)

        file_name_without_extension = task_uuid
        for file in files:
            if file.endswith(".ok"):
                file_name_without_extension = os.path.splitext(file)[0]
                break  # 只获取一个文件后退出循环

        amp_gene_path = os.path.join(output_dir, f'gistic_{file_name_without_extension}', 'amp_genes.conf_95.txt')
        del_gene_path = os.path.join(output_dir, f'gistic_{file_name_without_extension}', 'del_genes.conf_95.txt')
        scores_path = os.path.join(output_dir, f'gistic_{file_name_without_extension}', 'scores.gistic')

        amp_regions_info = recurrent_utils.parse_recurrent_regions(amp_gene_path)
        del_regions_info = recurrent_utils.parse_recurrent_regions(del_gene_path)
        recurrent_scores_data = recurrent_utils.parse_recurrent_scores(scores_path)

        if recurrent_scores_data is None:
            return Response({'error': 'No recurrent scores data found.'}, status=status.HTTP_400_BAD_REQUEST)

        return Response({
            'amp': amp_regions_info,
            'del': del_regions_info,
            'scores': recurrent_scores_data
        }, status=status.HTTP_200_OK)


class GeneRecurrenceQueryView(APIView):
    def get(self, request):
        page = request.query_params.get('page', None)
        page_size = request.query_params.get('page_size', None)
        task_uuid = request.query_params.get('uuid', None)
        
        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        
        try:
            page = int(page) if page else 1
            page_size = int(page_size) if page_size else 8
        except ValueError:
            return Response({'detail': 'Invalid page or page_size value.'}, status=status.HTTP_400_BAD_REQUEST)
        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        output_dir = task.get_output_dir_absolute_path()
        recurrence_file_path = os.path.join(output_dir, f'{task_uuid}_recurrent.json')

        try:
            with open(recurrence_file_path, 'r') as f:
                recurrence_data = json.load(f)
        except FileNotFoundError:
            return Response('Recurrence file not found!', status=status.HTTP_404_NOT_FOUND)

        # 获取所有的 dataset 键
        datasets = recurrence_data.get('datasets', {})
        dataset_keys = list(datasets.keys())

        # 排序 dataset_keys
        dataset_keys.sort()

        # 计算分页的开始和结束索引
        start_index = (page - 1) * page_size
        end_index = start_index + page_size

        # 获取当前页的 dataset 键
        paged_keys = dataset_keys[start_index:end_index]

        # 获取前缀
        prefix = f'{task_uuid}_'

        # 根据分页后的键获取对应的数据
        paged_datasets = {
            key.replace(prefix, ''): recurrent_utils.parse_recurrent_profiles(datasets[key])
            for key in paged_keys
        }

        return Response({
            'total': len(dataset_keys),
            'data': paged_datasets
        }, status=status.HTTP_200_OK)


class PloidyDistributionView(APIView):
    def get(self, request):
        task_uuid = request.query_params.get('uuid', None)
        
        if not task_uuid:
            return Response({'detail': 'Missing required parameter: task id.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            task = BasicAnnotationTask.objects.get(pk=task_uuid)
            task_type = "BasicAnnotationTask"
        except BasicAnnotationTask.DoesNotExist:
            # 如果不是BasicAnnotationTask，尝试查找RecurrentCNATask
            try:
                task = RecurrentCNATask.objects.get(pk=task_uuid)
                task_type = "RecurrentCNATask"
            except RecurrentCNATask.DoesNotExist:
                # 两种类型都不是，返回404
                return Response({
                    "success": False,
                    "msg": f"Task with UUID {task_uuid} not found in any task type"
                }, status=status.HTTP_404_NOT_FOUND)

        matrix_path = task.get_input_file_absolute_path()

        try:
            bin_abundance_list = matrix_utils.calculate_abundance(matrix_path)
        except FileNotFoundError:
            return Response({'error': 'Matrix file not found!'}, status=status.HTTP_404_NOT_FOUND)

        return Response(bin_abundance_list)

