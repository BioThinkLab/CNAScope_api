from database.models import Dataset
from database.utils import path_utils, matrix_utils, recurrent_utils

import os
import zipfile
import logging
import hashlib
from datetime import datetime
# def compress_existing_files(file_list, output_zip_path, empty_log_path=None):
#     """
#     检查文件列表中的文件是否存在，将存在的文件压缩成一个zip文件
#     处理重复文件名的情况，并将"没有找到任何文件"的情况记录到指定日志文件
    
#     参数:
#         file_list (list): 需要检查和压缩的文件路径列表
#         output_zip_path (str): 输出的zip文件路径
#         empty_log_path (str): 记录空ZIP情况的日志文件路径
    
#     返回:
#         bool: 如果至少有一个文件被压缩则返回True，否则返回False
#     """
#     # 配置常规日志记录
#     logger = logging.getLogger('compress_files')
#     logger.setLevel(logging.INFO)
    
#     # 清除现有的处理器，避免重复记录
#     if logger.handlers:
#         logger.handlers = []
    
#     # 添加控制台处理器
#     console_handler = logging.StreamHandler()
#     console_handler.setLevel(logging.INFO)
#     logger.addHandler(console_handler)
    
#     # 过滤出存在的文件
#     existing_files = []
#     for file_path in file_list:
#         if file_path and os.path.isfile(file_path):
#             existing_files.append(file_path)
#         else:
#             if file_path:
#                 logger.warning(f"文件不存在: {file_path}")
    
#     # 如果没有文件存在，记录到专门的空ZIP日志文件
#     if not existing_files:
#         logger.warning(f"没有找到可压缩的文件，不创建zip文件: {output_zip_path}")
        
#         # 将空ZIP情况记录到指定的日志文件
#         if empty_log_path:
#             # 追加模式打开日志文件
#             with open(empty_log_path, 'a') as empty_log:
#                 current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 empty_log.write(f"[{current_time}] 空ZIP文件未生成: {output_zip_path}\n")
        
#         return False
    
#     try:
#         # 创建用于跟踪文件名的字典
#         file_names = {}
        
#         # 创建zip文件
#         with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
#             for file_path in existing_files:
#                 # 获取基本文件名
#                 base_name = os.path.basename(file_path)
                
#                 # 检查文件名是否已存在
#                 if base_name in file_names.values():
#                     # 使用路径的一部分创建唯一文件名
#                     dir_name = os.path.dirname(file_path)
#                     hash_prefix = hashlib.md5(dir_name.encode()).hexdigest()[:8]
#                     unique_name = f"{hash_prefix}_{base_name}"
                    
#                     # 如果仍然冲突，添加索引号
#                     original_unique_name = unique_name
#                     counter = 1
#                     while unique_name in file_names.values():
#                         unique_name = f"{original_unique_name}_{counter}"
#                         counter += 1
                    
#                     # 保存文件并记录唯一文件名
#                     zipf.write(file_path, unique_name)
#                     file_names[file_path] = unique_name
#                     logger.info(f"已添加文件到压缩包(重命名): {file_path} -> {unique_name}")
#                 else:
#                     # 直接保存文件并记录文件名
#                     zipf.write(file_path, base_name)
#                     file_names[file_path] = base_name
#                     logger.info(f"已添加文件到压缩包: {file_path}")
        
#         logger.info(f"压缩完成，文件保存在: {output_zip_path}")
#         logger.info(f"总共压缩了 {len(existing_files)} 个文件")
#         return True
    
#     except Exception as e:
#         logger.error(f"压缩文件时出错: {str(e)}")
        
#         # 将错误情况也记录到空ZIP日志文件
#         if empty_log_path:
#             with open(empty_log_path, 'a') as empty_log:
#                 current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 empty_log.write(f"[{current_time}] ZIP文件创建失败: {output_zip_path} - 错误: {str(e)}\n")
        
#         return False

def compress_existing_files(files_config, output_zip_path, empty_log_path=None):
    """
    根据配置将文件压缩成zip文件，并支持自定义ZIP内的目录结构
    
    参数:
        files_config (list): 文件配置列表，每项可以是:
                            - 字符串: 文件路径，将直接添加到ZIP根目录
                            - 元组: (文件路径, ZIP内目标路径)，将文件放在ZIP中指定位置
        output_zip_path (str): 输出的zip文件路径
        empty_log_path (str): 记录空ZIP情况的日志文件路径
    
    返回:
        bool: 如果至少有一个文件被压缩则返回True，否则返回False
    """
    # 配置常规日志记录
    logger = logging.getLogger('compress_files')
    logger.setLevel(logging.INFO)
    
    # 清除现有的处理器，避免重复记录
    if logger.handlers:
        logger.handlers = []
    
    # 添加控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    
    # 标准化文件配置
    normalized_configs = []
    for item in files_config:
        if isinstance(item, tuple):
            file_path, target_path = item
            if file_path and os.path.exists(file_path):
                if os.path.isfile(file_path):
                    normalized_configs.append((file_path, target_path))
                elif os.path.isdir(file_path):
                    # 处理文件夹，递归添加所有子文件
                    for root, _, files in os.walk(file_path):
                        for file in files:
                            full_path = os.path.join(root, file)
                            # 计算相对路径并拼接到目标路径
                            rel_path = os.path.relpath(full_path, os.path.dirname(file_path))
                            zip_path = os.path.join(target_path, rel_path)
                            normalized_configs.append((full_path, zip_path))
            else:
                if file_path:
                    logger.warning(f"文件不存在: {file_path}")
        else:
            # 单个文件路径，直接放在根目录
            file_path = item
            if file_path and os.path.exists(file_path):
                if os.path.isfile(file_path):
                    # 直接使用文件名作为目标路径（放在根目录）
                    normalized_configs.append((file_path, os.path.basename(file_path)))
                elif os.path.isdir(file_path):
                    # 处理文件夹，递归添加所有子文件到根目录对应的子文件夹
                    for root, _, files in os.walk(file_path):
                        for file in files:
                            full_path = os.path.join(root, file)
                            # 计算相对路径
                            rel_path = os.path.relpath(full_path, os.path.dirname(file_path))
                            normalized_configs.append((full_path, rel_path))
            else:
                if file_path:
                    logger.warning(f"文件不存在: {file_path}")
    
    # 如果没有有效文件可压缩
    if not normalized_configs:
        logger.warning(f"没有找到可压缩的文件，不创建zip文件: {output_zip_path}")
        
        # 将空ZIP情况记录到指定的日志文件
        if empty_log_path:
            with open(empty_log_path, 'a') as empty_log:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                empty_log.write(f"[{current_time}] 空ZIP文件未生成: {output_zip_path}\n")
        
        return False
    
    try:
        # 创建用于跟踪文件名的字典，确保在同一目录不会有重复文件名
        path_name_map = {}
        
        # 创建zip文件
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path, target_path in normalized_configs:
                # 检查目标路径是否有同名文件
                target_dir = os.path.dirname(target_path) if os.path.dirname(target_path) else ""
                base_name = os.path.basename(target_path)
                
                if target_dir not in path_name_map:
                    path_name_map[target_dir] = set()
                
                if base_name in path_name_map[target_dir]:
                    # 文件名冲突，生成唯一名称
                    hash_prefix = hashlib.md5(file_path.encode()).hexdigest()[:8]
                    unique_name = f"{hash_prefix}_{base_name}"
                    
                    # 继续检查直到找到唯一名称
                    counter = 1
                    while unique_name in path_name_map[target_dir]:
                        unique_name = f"{hash_prefix}_{base_name}_{counter}"
                        counter += 1
                    
                    # 更新目标路径
                    if target_dir:
                        target_path = os.path.join(target_dir, unique_name)
                    else:
                        target_path = unique_name
                
                # 记录文件名以检测后续冲突
                path_name_map[target_dir].add(os.path.basename(target_path))
                
                # 添加文件到ZIP
                zipf.write(file_path, target_path)
                logger.info(f"已添加文件到压缩包: {file_path} -> {target_path}")
        
        logger.info(f"压缩完成，文件保存在: {output_zip_path}")
        logger.info(f"总共压缩了 {len(normalized_configs)} 个文件")
        return True
    
    except Exception as e:
        logger.error(f"压缩文件时出错: {str(e)}")
        
        # 将错误情况也记录到空ZIP日志文件
        if empty_log_path:
            with open(empty_log_path, 'a') as empty_log:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                empty_log.write(f"[{current_time}] ZIP文件创建失败: {output_zip_path} - 错误: {str(e)}\n")
        
        return False
    
empty_log = "/home/platform/workspace/CNAScope/data/download_zips/empty_zip_log.txt"
bin_sizes = ['200kb', '500kb', '5M']
cn_type_map = {
    'allele': 'allele-specific',
    'cns': 'copy-number-segment',
    'mcns': 'masked-copy-number-segment'
}
for dataset in Dataset.objects.all():
    name = dataset.name
    workflow = dataset.workflow
    source = dataset.source

    if ',' in workflow:
        all_files = []
        for w in workflow.split(','):
            if source == 'GDC Portal':
                for bin_size in bin_sizes:
                    cna_file = path_utils.get_dataset_matrix_path(dataset, w, bin_size)
                    meta_file = path_utils.get_dataset_meta_path(dataset, w, bin_size)
                    newick_file = path_utils.get_dataset_newick_path(dataset, w, bin_size)
                    gene_matrix_file = path_utils.get_dataset_gene_matrix_csv_path(dataset, w, bin_size)
                    term_matrix_file = path_utils.get_dataset_term_matrix_csv_path(dataset, w, bin_size)
                    top_cn_file = path_utils.get_dataset_top_cn_variance_path(dataset, w, bin_size)

                    all_files.append((meta_file, os.path.join(bin_size, os.path.basename(cna_file))))
                    all_files.append((cna_file, os.path.join(bin_size, os.path.basename(cna_file))))
                    all_files.append((newick_file, os.path.join(bin_size, os.path.basename(newick_file))))
                    all_files.append((gene_matrix_file, os.path.join(bin_size, os.path.basename(gene_matrix_file))))
                    all_files.append((term_matrix_file, os.path.join(bin_size, os.path.basename(term_matrix_file))))
                    all_files.append((top_cn_file, os.path.join(bin_size, os.path.basename(top_cn_file))))
                for cn_type in cn_type_map.values():
                    amp_gene_path = path_utils.get_dataset_recurrent_gene_path(dataset, cn_type, w, 'amp')
                    del_gene_path = path_utils.get_dataset_recurrent_gene_path(dataset, cn_type, w, 'del')
                    scores_path = path_utils.get_dataset_recurrent_scores_path(dataset, cn_type, w)
                    seg_path = path_utils.get_dataset_recurrent_seg_path(dataset, cn_type, w)
                    ora_csv_path = path_utils.get_ora_csv_path(name, cn_type, w)

                    all_files.append((amp_gene_path, os.path.join(cn_type, os.path.basename(amp_gene_path))))
                    all_files.append((del_gene_path, os.path.join(cn_type, os.path.basename(del_gene_path))))
                    all_files.append((scores_path, os.path.join(cn_type, os.path.basename(scores_path))))
                    all_files.append((seg_path, os.path.join(cn_type, os.path.basename(seg_path))))
                    all_files.append((ora_csv_path, os.path.join(cn_type, os.path.basename(ora_csv_path))))
                
                consensus_cna = path_utils.get_consensus_cna_csv_path(name, w)
                consensus_gene = path_utils.get_consensus_gene_csv_path(name, w)
                consensus_term = path_utils.get_consensus_term_csv_path(name, w)
                consensus_focal = path_utils.get_ora_csv_path(name, 'consensus', 'consensus')

                all_files.append((consensus_cna, os.path.join('consensus', os.path.basename(consensus_cna))))
                all_files.append((consensus_gene, os.path.join('consensus', os.path.basename(consensus_gene))))
                all_files.append((consensus_term, os.path.join('consensus', os.path.basename(consensus_term))))
                all_files.append((consensus_focal, os.path.join('consensus', os.path.basename(consensus_focal))))
            else:
                cna_file = path_utils.get_dataset_matrix_path(dataset, w, '')
                meta_file = path_utils.get_dataset_meta_path(dataset, w, '')
                newick_file = path_utils.get_dataset_newick_path(dataset, w, '')
                gene_matrix_file = path_utils.get_dataset_gene_matrix_csv_path(dataset, w, '')
                term_matrix_file = path_utils.get_dataset_term_matrix_csv_path(dataset, w, '')
                top_cn_file = path_utils.get_dataset_top_cn_variance_path(dataset, w, '')

                all_files.append((meta_file, os.path.basename(cna_file)))
                all_files.append((cna_file, os.path.basename(cna_file)))
                all_files.append((newick_file, os.path.basename(newick_file)))
                all_files.append((gene_matrix_file, os.path.basename(gene_matrix_file)))
                all_files.append((term_matrix_file, os.path.basename(term_matrix_file)))
                all_files.append((top_cn_file, os.path.basename(top_cn_file)))

                if dataset.modality in ['spaDNA', 'spaRNA']:
                    spatial_file = path_utils.get_dataset_spatial_top_cn_variance_path(dataset, w, '')
                    all_files.append((spatial_file, os.path.basename(spatial_file)))
    else:
        all_files = []
        if source == 'GDC Portal':
            for bin_size in bin_sizes:
                cna_file = path_utils.get_dataset_matrix_path(dataset, workflow, bin_size)
                meta_file = path_utils.get_dataset_meta_path(dataset, workflow, bin_size)
                newick_file = path_utils.get_dataset_newick_path(dataset, workflow, bin_size)
                gene_matrix_file = path_utils.get_dataset_gene_matrix_csv_path(dataset, workflow, bin_size)
                term_matrix_file = path_utils.get_dataset_term_matrix_csv_path(dataset, workflow, bin_size)
                top_cn_file = path_utils.get_dataset_top_cn_variance_path(dataset, workflow, bin_size)

                all_files.append((meta_file, os.path.join(bin_size, os.path.basename(cna_file))))
                all_files.append((cna_file, os.path.join(bin_size, os.path.basename(cna_file))))
                all_files.append((newick_file, os.path.join(bin_size, os.path.basename(newick_file))))
                all_files.append((gene_matrix_file, os.path.join(bin_size, os.path.basename(gene_matrix_file))))
                all_files.append((term_matrix_file, os.path.join(bin_size, os.path.basename(term_matrix_file))))
                all_files.append((top_cn_file, os.path.join(bin_size, os.path.basename(top_cn_file))))
            for cn_type in cn_type_map.values():
                amp_gene_path = path_utils.get_dataset_recurrent_gene_path(dataset, cn_type, workflow, 'amp')
                del_gene_path = path_utils.get_dataset_recurrent_gene_path(dataset, cn_type, workflow, 'del')
                scores_path = path_utils.get_dataset_recurrent_scores_path(dataset, cn_type, workflow)
                seg_path = path_utils.get_dataset_recurrent_seg_path(dataset, cn_type, workflow)
                ora_csv_path = path_utils.get_ora_csv_path(name, cn_type, workflow)

                all_files.append((amp_gene_path, os.path.join(cn_type, os.path.basename(amp_gene_path))))
                all_files.append((del_gene_path, os.path.join(cn_type, os.path.basename(del_gene_path))))
                all_files.append((scores_path, os.path.join(cn_type, os.path.basename(scores_path))))
                all_files.append((seg_path, os.path.join(cn_type, os.path.basename(seg_path))))
                all_files.append((ora_csv_path, os.path.join(cn_type, os.path.basename(ora_csv_path))))
            
            consensus_cna = path_utils.get_consensus_cna_csv_path(name, workflow)
            consensus_gene = path_utils.get_consensus_gene_csv_path(name, workflow)
            consensus_term = path_utils.get_consensus_term_csv_path(name, workflow)
            consensus_focal = path_utils.get_ora_csv_path(name, 'consensus', 'consensus')

            all_files.append((consensus_cna, os.path.join('consensus', os.path.basename(consensus_cna))))
            all_files.append((consensus_gene, os.path.join('consensus', os.path.basename(consensus_gene))))
            all_files.append((consensus_term, os.path.join('consensus', os.path.basename(consensus_term))))
            all_files.append((consensus_focal, os.path.join('consensus', os.path.basename(consensus_focal))))
        else:
            cna_file = path_utils.get_dataset_matrix_path(dataset, workflow, '')
            meta_file = path_utils.get_dataset_meta_path(dataset, workflow, '')
            newick_file = path_utils.get_dataset_newick_path(dataset, workflow, '')
            gene_matrix_file = path_utils.get_dataset_gene_matrix_csv_path(dataset, workflow, '')
            term_matrix_file = path_utils.get_dataset_term_matrix_csv_path(dataset, workflow, '')
            top_cn_file = path_utils.get_dataset_top_cn_variance_path(dataset, workflow, '')

            all_files.append((meta_file, os.path.basename(cna_file)))
            all_files.append((cna_file, os.path.basename(cna_file)))
            all_files.append((newick_file, os.path.basename(newick_file)))
            all_files.append((gene_matrix_file, os.path.basename(gene_matrix_file)))
            all_files.append((term_matrix_file, os.path.basename(term_matrix_file)))
            all_files.append((top_cn_file, os.path.basename(top_cn_file)))

            if dataset.modality in ['spaDNA', 'spaRNA']:
                spatial_file = path_utils.get_dataset_spatial_top_cn_variance_path(dataset, workflow, '')
                all_files.append((spatial_file, os.path.basename(spatial_file)))

    zip_file_name = os.path.join('/home/platform/workspace/CNAScope/data/download_zips', f'{name}.zip')
    compress_existing_files(all_files, zip_file_name, empty_log)
