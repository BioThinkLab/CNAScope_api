from database.models import Dataset
from database.utils import path_utils, matrix_utils, recurrent_utils

import os
import zipfile
import logging

def compress_existing_files(file_list, output_zip_path):
    """
    检查文件列表中的文件是否存在，将存在的文件压缩成一个zip文件
    
    参数:
        file_list (list): 需要检查和压缩的文件路径列表
        output_zip_path (str): 输出的zip文件路径
    
    返回:
        bool: 如果至少有一个文件被压缩则返回True，否则返回False
    """
    # 过滤出存在的文件
    existing_files = []
    for file_path in file_list:
        if file_path and os.path.isfile(file_path):
            existing_files.append(file_path)
        # else:
        #     if file_path:
        #         logging.warning(f"文件不存在: {file_path}")
    
    # 如果没有文件存在，返回False
    if not existing_files:
        print(file_list)
        print(output_zip_path)
        logging.warning("没有找到可压缩的文件")
        return False
    
    try:
        # 创建zip文件
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in existing_files:
                # 获取文件名，不包含路径
                file_name = os.path.basename(file_path)
                # 将文件添加到zip中
                zipf.write(file_path, file_name)
                # logging.info(f"已添加文件到压缩包: {file_path}")
        
        # logging.info(f"压缩完成，文件保存在: {output_zip_path}")
        return True
    
    except Exception as e:
        logging.error(f"压缩文件时出错: {str(e)}")
        return False

for dataset in Dataset.objects.all():
    name = dataset.name
    workflow = dataset.workflow
    source = dataset.source

    if ',' in workflow:
        all_files = []
        for w in workflow.split(','):
            cna_file = path_utils.get_dataset_matrix_path(dataset, w)
            meta_file = path_utils.get_dataset_meta_path(dataset, w)
            newick_file = path_utils.get_dataset_newick_path(dataset, w)
            gene_matrix_file = path_utils.get_dataset_gene_matrix_csv_path(dataset, w)
            term_matrix_file = path_utils.get_dataset_term_matrix_csv_path(dataset, w)
            score_file = path_utils.get_dataset_recurrent_scores_path(dataset, w)
            amp_file = path_utils.get_dataset_recurrent_gene_path(dataset, w, 'amp')
            del_file = path_utils.get_dataset_recurrent_gene_path(dataset, w, 'del')

            all_files += [
                cna_file,
                meta_file,
                newick_file,
                gene_matrix_file,
                term_matrix_file,
                score_file,
                amp_file,
                del_file
            ]
    else:
        cna_file = path_utils.get_dataset_matrix_path(dataset, workflow)
        meta_file = path_utils.get_dataset_meta_path(dataset, workflow)
        newick_file = path_utils.get_dataset_newick_path(dataset, workflow)
        gene_matrix_file = path_utils.get_dataset_gene_matrix_csv_path(dataset, workflow)
        term_matrix_file = path_utils.get_dataset_term_matrix_csv_path(dataset, workflow)
        score_file = path_utils.get_dataset_recurrent_scores_path(dataset, workflow)
        amp_file = path_utils.get_dataset_recurrent_gene_path(dataset, workflow, 'amp')
        del_file = path_utils.get_dataset_recurrent_gene_path(dataset, workflow, 'del')

        all_files = [
            cna_file,
            meta_file,
            newick_file,
            gene_matrix_file,
            term_matrix_file,
            score_file,
            amp_file,
            del_file
        ]

    zip_file_name = os.path.join('/home/platform/workspace/CNAScope/data/download_zips', f'{name}.zip')
    compress_existing_files(all_files, zip_file_name)
