from database.models import Dataset
from database.utils import path_utils, matrix_utils, recurrent_utils

import os
import zipfile
import logging
import hashlib
from datetime import datetime
def compress_existing_files(file_list, output_zip_path, log_file_path=None):
    """
    检查文件列表中的文件是否存在，将存在的文件压缩成一个zip文件
    处理重复文件名的情况，并将日志记录到指定文件
    
    参数:
        file_list (list): 需要检查和压缩的文件路径列表
        output_zip_path (str): 输出的zip文件路径
        log_file_path (str): 日志文件路径，如果为None则不记录到文件
    
    返回:
        bool: 如果至少有一个文件被压缩则返回True，否则返回False
    """
    # 配置日志记录
    logger = logging.getLogger('compress_files')
    logger.setLevel(logging.INFO)
    
    # 清除现有的处理器，避免重复记录
    logger.handlers = []
    
    # 添加控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    
    # 如果提供了日志文件路径，添加文件处理器
    if log_file_path:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
    
    # 记录开始压缩的时间和目标zip文件
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"开始压缩文件 [{current_time}] - 目标文件: {output_zip_path}")
    
    # 过滤出存在的文件
    existing_files = []
    for file_path in file_list:
        if file_path and os.path.isfile(file_path):
            existing_files.append(file_path)
        else:
            if file_path:
                logger.warning(f"文件不存在: {file_path}")
    
    # 如果没有文件存在，记录到日志并返回False
    if not existing_files:
        logger.warning(f"没有找到可压缩的文件，不创建zip文件: {output_zip_path}")
        # 专门记录空输出路径到日志
        logger.error(f"空ZIP文件未生成: {output_zip_path}")
        return False
    
    try:
        # 创建用于跟踪文件名的字典
        file_names = {}
        
        # 创建zip文件
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in existing_files:
                # 获取基本文件名
                base_name = os.path.basename(file_path)
                
                # 检查文件名是否已存在
                if base_name in file_names.values():
                    # 使用路径的一部分创建唯一文件名
                    dir_name = os.path.dirname(file_path)
                    hash_prefix = hashlib.md5(dir_name.encode()).hexdigest()[:8]
                    unique_name = f"{hash_prefix}_{base_name}"
                    
                    # 如果仍然冲突，添加索引号
                    original_unique_name = unique_name
                    counter = 1
                    while unique_name in file_names.values():
                        unique_name = f"{original_unique_name}_{counter}"
                        counter += 1
                    
                    # 保存文件并记录唯一文件名
                    zipf.write(file_path, unique_name)
                    file_names[file_path] = unique_name
                    logger.info(f"已添加文件到压缩包(重命名): {file_path} -> {unique_name}")
                else:
                    # 直接保存文件并记录文件名
                    zipf.write(file_path, base_name)
                    file_names[file_path] = base_name
                    logger.info(f"已添加文件到压缩包: {file_path}")
        
        logger.info(f"压缩完成，文件保存在: {output_zip_path}")
        logger.info(f"总共压缩了 {len(existing_files)} 个文件")
        return True
    
    except Exception as e:
        logger.error(f"压缩文件时出错: {str(e)}")
        # 记录失败的输出路径
        logger.error(f"ZIP文件创建失败: {output_zip_path}")
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
