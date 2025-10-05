import os
import csv
import json

import pyarrow.parquet as pq
import pandas as pd

from django.http import HttpResponse, FileResponse

from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response

from database.models import Dataset

from database.utils import path_utils, matrix_utils, recurrent_utils


class CNAMatrixView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)
        bin_size = request.query_params.get('bin_size', None)

        if not dataset_name or not workflow_type or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        matrix_path = path_utils.get_dataset_matrix_path(dataset, workflow_type, bin_size)

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
        bin_size = request.query_params.get('bin_size', None)

        if not dataset_name or not workflow_type or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        meta_path = path_utils.get_dataset_meta_path(dataset, workflow_type, bin_size)

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
        bin_size = request.query_params.get('bin_size', None)

        if not dataset_name or not workflow_type or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        tree_path = path_utils.get_dataset_tree_path(dataset, workflow_type, bin_size)

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
        bin_size = request.query_params.get('bin_size', None)

        if not dataset_name or not workflow_type or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        gene_matrix_path = path_utils.get_dataset_gene_matrix_path(dataset, workflow_type, bin_size)

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
        bin_size = request.query_params.get('bin_size', None)

        if not dataset_name or not workflow_type or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        newick_path = path_utils.get_dataset_newick_path(dataset, workflow_type, bin_size)

        try:
            with open(newick_path, 'r') as file:
                content = file.read()
        except FileNotFoundError:
            return Response('Newick file not found!', status=status.HTTP_404_NOT_FOUND)

        return Response(content)


class CNAGeneMatrixView(APIView):
    def post(self, request):
        dataset_name = request.data.get('datasetName', None)
        workflow_type = request.data.get('workflowType', None)
        genes = request.data.get('genes', None)
        bin_size = request.data.get('binSize', None)

        # 基本参数校验
        if not dataset_name:
            return Response({'error': 'datasetName is required'}, status=status.HTTP_400_BAD_REQUEST)
        if not workflow_type:
            return Response({'error': 'workflowType is required'}, status=status.HTTP_400_BAD_REQUEST)
        if not isinstance(genes, list):
            return Response({'error': 'genes must be a list'}, status=status.HTTP_400_BAD_REQUEST)
        if not bin_size:
            return Response({'error': 'binSize is required'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'error': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        gene_matrix_path = path_utils.get_dataset_gene_matrix_path(dataset, workflow_type, bin_size)

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
        bin_size = request.query_params.get('bin_size', None)

        if not dataset_name or not workflow_type or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'detail': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        term_matrix_path = path_utils.get_dataset_term_matrix_path(dataset, workflow_type, bin_size)

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
        bin_size = request.data.get('binSize', None)

        if not dataset_name or not workflow_type or not terms or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        if not isinstance(terms, list):
            return Response({'error': 'terms must be a list'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'error': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        term_matrix_path = path_utils.get_dataset_term_matrix_path(dataset, workflow_type, bin_size)

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


class FocalCNAOptionsView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)

        if not dataset_name:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        result = recurrent_utils.get_gistic_options(dataset_name)

        return Response(result)


class FocalCNAInfoView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        cn_type = request.query_params.get('cn_type', None)
        workflow_type = request.query_params.get('workflow_type', None)

        if not dataset_name or not workflow_type or not cn_type:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'error': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        amp_gene_path = path_utils.get_dataset_recurrent_gene_path(dataset, cn_type, workflow_type, 'amp')
        del_gene_path = path_utils.get_dataset_recurrent_gene_path(dataset, cn_type, workflow_type, 'del')
        scores_path = path_utils.get_dataset_recurrent_scores_path(dataset, cn_type, workflow_type)

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
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)
        page = request.query_params.get('page', None)
        page_size = request.query_params.get('page_size', None)
        bin_size = request.query_params.get('bin_size', None)

        if not dataset_name or not workflow_type or not page or not page_size or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            page = int(page) if page else 1
            page_size = int(page_size) if page_size else 8
        except ValueError:
            return Response({'detail': 'Invalid page or page_size value.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'error': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        recurrence_file_path = path_utils.get_dataset_recurrent_json_path(dataset, workflow_type, bin_size)

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
        prefix = path_utils.build_dataset_prefix(dataset, workflow_type)

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
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)
        bin_size = request.query_params.get('bin_size', None)

        if not dataset_name or not workflow_type or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'error': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        matrix_path = path_utils.get_dataset_matrix_path(dataset, workflow_type, bin_size)

        try:
            bin_abundance_list = matrix_utils.calculate_abundance(matrix_path)
        except FileNotFoundError:
            return Response({'error': 'Matrix file not found!'}, status=status.HTTP_404_NOT_FOUND)

        return Response(bin_abundance_list)


class TopCNVarianceView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)
        bin_size = request.query_params.get('bin_size', None)

        if not dataset_name or not workflow_type or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'error': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        matrix_path = path_utils.get_dataset_top_cn_variance_path(dataset, workflow_type, bin_size)

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


class SpatialTopCNVarianceView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        workflow_type = request.query_params.get('workflow_type', None)
        bin_size = request.query_params.get('bin_size', None)

        if not dataset_name or not workflow_type or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'error': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        matrix_path = path_utils.get_dataset_spatial_top_cn_variance_path(dataset, workflow_type, bin_size)

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


class CNAVectorView(APIView):
    def post(self, request):
        dataset_name = request.data.get('datasetName', None)
        workflow_type = request.data.get('workflowType', None)
        bins = request.data.get('bins', None)
        bin_size = request.data.get('binSize', None)

        if not dataset_name or not workflow_type or not bins or not bin_size:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        if not isinstance(bins, list):
            return Response({'error': 'bins must be a list'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dataset = Dataset.objects.get(name=dataset_name)
        except Dataset.DoesNotExist:
            return Response({'error': 'Dataset does not exist.'}, status=status.HTTP_400_BAD_REQUEST)

        bin_matrix_path = path_utils.get_dataset_matrix_path(dataset, workflow_type, bin_size)

        try:
            # 提取 Term 的 CNA 矩阵
            df = matrix_utils.extract_matrix_from_csv(bin_matrix_path, bins)

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


class CNAConsensusVennView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)

        if not dataset_name:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        focal_gene_json_path = path_utils.get_consensus_focal_gene_json_path(dataset_name)

        try:
            with open(focal_gene_json_path, 'r') as f:
                focal_gene_data = json.load(f)

                return Response(focal_gene_data)
        except FileNotFoundError:
            return Response('Consensus Focal Gene file not found!', status=status.HTTP_404_NOT_FOUND)


class CNAConsensusGeneView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)

        if not dataset_name:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        consensus_gene_csv_path = path_utils.get_consensus_gene_csv_path(dataset_name)

        try:
            consensus_gene_df = pd.read_csv(consensus_gene_csv_path)

            # 准备转换后的数据存储
            converted_data = []

            # 遍历每一行数据并构建字典
            for _, row in consensus_gene_df.iterrows():
                data = {
                    "type": row["CNA_Type"],
                    "modality_workflow": row["Protocol_Workflow"],
                    "n_modality_workflow": row["n_Protocol_Workflow"],
                    "consensus_gene": row["consensus_gene"],
                    "n_consensus": row["n_consensus"]
                }

                # 将构建好的字典添加到结果列表
                converted_data.append(data)

            return Response(converted_data)
        except FileNotFoundError:
            return Response('Consensus Gene file not found!', status=status.HTTP_404_NOT_FOUND)


class CNAConsensusGeneDownloadView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)

        if not dataset_name:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        consensus_gene_csv_path = path_utils.get_consensus_gene_csv_path(dataset_name)

        # 检查文件是否存在
        if not os.path.exists(consensus_gene_csv_path):
            return Response({'detail': 'File not found.'}, status=status.HTTP_404_NOT_FOUND)

        # 读取文件并返回响应，作为文件下载
        try:
            response = FileResponse(open(consensus_gene_csv_path, 'rb'), as_attachment=True,
                                    filename=f"{dataset_name}_consensus_genes.csv")
            return response
        except Exception as e:
            return Response({'detail': f'Error reading file: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class PathwayEnrichmentPlotOptionsView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)

        if not dataset_name:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        result = recurrent_utils.get_ora_options(dataset_name)

        return Response(result)


class PathwayEnrichmentPlotView(APIView):
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name', None)
        cn_type = request.query_params.get('cn_type', None)
        workflow = request.query_params.get('workflow', None)

        if not dataset_name or not cn_type and not workflow:
            return Response({'detail': 'Missing required parameters.'}, status=status.HTTP_400_BAD_REQUEST)

        ora_csv_path = path_utils.get_ora_csv_path(dataset_name, cn_type, workflow)

        try:
            ora_df = pd.read_csv(ora_csv_path)
            filtered_df = ora_df[ora_df["Adjusted P-value"] < 0.05]
            records = filtered_df.to_dict(orient="records")

            return Response(records)
        except FileNotFoundError:
            return Response('Pathway Enrichment file not found!', status=status.HTTP_404_NOT_FOUND)

