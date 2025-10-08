import os
import re
import glob

from CNAScope_api.constant import DATA_HOME, GISTIC_HOME

source_map = {
    'cBioportal': 'cBioPortal',
    'COSMIC': 'COSMIC',
    'GDC Portal': 'GDC',
    '10x Official': '10x',
    'HSCGD': 'HSCGD',
    'scTML': 'scTML',
}

modality_map = {
    'ST': 'spaRNA'
}

workflow_map = {
    'ASCAT2': 'ascat2',
    'ASCAT3': 'ascat3',
    'AscatNGS': 'ascatNGS',
    'GATK4 CNV': 'GATK4_CNV',
    'Cell Ranger DNA': 'Cell_Ranger_DNA',
    'Ginkgo': 'Ginkgo',
    'InferCNV': 'InferCNV',
    'Affymetrix 250K SNP array': 'Affymetrix-250K-SNP-array',
    'Affymetrix SNP 6.0': 'Affymetrix-SNP-6.0',
    'AllelicCapSeg': 'AllelicCapSeg',
    'ASCAT-GISTIC2': 'ASCAT-GISTIC2',
    'Broad_Firehose': 'Broad_Firehose',
    'CCLE': 'CCLE',
    'CNVEX': 'CNVEX',
    'CNVkit': 'CNVkit',
    'DNAcopy': 'DNAcopy',
    'ExomeCNV-GISTIC2': 'ExomeCNV-GISTIC2',
    'FACETS': 'FACETS',
    'GATK_CNV': 'GATK_CNV',
    'GISTIC': 'GISTIC',
    'GISTIC2': 'GISTIC2',
    'HMMcopy': 'HMMcopy',
    'IMPACT': 'IMPACT',
    'IMPACT341': 'IMPACT341',
    'IPM-Exome-pipeline': 'IPM-Exome-pipeline',
    'PatternCNV': 'PatternCNV',
    'ReCapSeg': 'ReCapSeg',
    'Sequenza': 'Sequenza',
    'Sequenza-CNVkit': 'Sequenza-CNVkit',
    'SNP-FASST2': 'SNP-FASST2',
    'tCoNut': 'tCoNut',
    'Slide-DNA-Seq': 'Slide_DNA_Seq',
    'NA': 'nan',
    '': 'nan',
}

cn_type_map = {
    'allele': 'allele-specific',
    'cns': 'copy-number-segment',
    'mcns': 'masked-copy-number-segment'
}

ora_workflow_map = {
    'Ascat2': 'ascat2',
    'Ascat3': 'ascat3',
    'AscatNGS': 'ascatNGS',
    'GATK4 CNV': 'GATK4_CNV',
    'DNAcopy': 'DNAcopy'
}


def build_dataset_data_dir_path(dataset):
    if dataset.source == '10x Official' and dataset.modality == 'spaRNA':
        dir_name = '10x_Xenium'
    elif dataset.protocol == 'Slide-RNA-Seq v2' or dataset.protocol == 'Slide-DNA-Seq':
        dir_name = 'slide'
    else:
        dir_name = source_map[dataset.source]

    return os.path.join(DATA_HOME, modality_map.get(dataset.modality, dataset.modality), dir_name)


def build_dataset_prefix(dataset, workflow):
    workflow_name = workflow_map.get(workflow, workflow)

    return f'{dataset.name}.{workflow_name}_'


def get_dataset_samples_path(dataset):
    data_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        meta_name = f'{dataset.name}.meta.csv'

        return os.path.join(data_dir, 'clean', meta_name)

    else:
        workflow_name = workflow_map.get(dataset.workflow, dataset.workflow)
        meta_name = f'{dataset.name}.{workflow_name}.meta.csv'

        return os.path.join(data_dir, 'clean', meta_name)


def get_dataset_matrix_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'clean', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'clean')

    workflow_name = workflow_map.get(workflow, workflow)
    matrix_name = f'{dataset.name}.{workflow_name}.cna.csv'

    return os.path.join(data_dir, matrix_name)


def get_dataset_meta_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    meta_name = f'{dataset.name}.{workflow_name}_meta_scsvas.csv'

    return os.path.join(data_dir, meta_name)


def get_dataset_tree_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    pattern = f"^{dataset.name}\.{workflow_name}_cut.*\.json$"

    # 使用glob查找所有文件
    files = [f for f in os.listdir(data_dir) if f.endswith('.json')]

    # 匹配符合模式的文件
    matched_files = [f for f in files if re.match(pattern, f)]

    if matched_files:
        # 获取第一个匹配文件的名称
        first_matched_file = matched_files[0]

        return os.path.join(data_dir, first_matched_file)
    else:
        return None


def get_dataset_gene_matrix_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    gene_matrix_name = f'{dataset.name}.{workflow_name}_gene_cna.parquet'

    return os.path.join(data_dir, gene_matrix_name)


def get_dataset_gene_matrix_csv_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    gene_matrix_name = f'{dataset.name}.{workflow_name}_gene_cna.csv.gz'

    return os.path.join(data_dir, gene_matrix_name)


def get_dataset_newick_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    newick_name = f'{dataset.name}.{workflow_name}.nwk'

    return os.path.join(data_dir, newick_name)


def get_dataset_term_matrix_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    term_matrix_name = f'{dataset.name}.{workflow_name}_term_cna.parquet'

    return os.path.join(data_dir, term_matrix_name)


def get_dataset_term_matrix_csv_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    term_matrix_name = f'{dataset.name}.{workflow_name}_term_cna.csv.gz'

    return os.path.join(data_dir, term_matrix_name)


def get_dataset_recurrent_scores_path(dataset, cn_type, workflow):
    cn_type_folder_name = cn_type_map.get(cn_type, cn_type)
    data_dir = os.path.join(str(GISTIC_HOME), cn_type_folder_name)

    workflow_name = ora_workflow_map.get(workflow, workflow)
    folder_name = f'gistic_{dataset.name}.{workflow_name}'

    return os.path.join(data_dir, folder_name, 'scores.gistic')

def get_dataset_recurrent_seg_path(dataset, cn_type, workflow):
    cn_type_folder_name = cn_type_map.get(cn_type, cn_type)
    data_dir = os.path.join(str(GISTIC_HOME), cn_type_folder_name)

    workflow_name = ora_workflow_map.get(workflow, workflow)
    folder_name = f'gistic_{dataset.name}.{workflow_name}'

    return os.path.join(data_dir, folder_name, 'gistic_seg.txt')

def get_dataset_recurrent_gene_path(dataset, cn_type, workflow, recurrent_type):
    cn_type_folder_name = cn_type_map.get(cn_type, cn_type)
    data_dir = os.path.join(str(GISTIC_HOME), cn_type_folder_name)

    workflow_name = ora_workflow_map.get(workflow, workflow)
    folder_name = f'gistic_{dataset.name}.{workflow_name}'
    file_name = f'{recurrent_type}_genes.conf_95.txt'

    return os.path.join(data_dir, folder_name, file_name)


def get_dataset_recurrent_json_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    file_name = f'{dataset.name}.{workflow_name}_recurrent.json'

    return os.path.join(data_dir, file_name)


def get_dataset_top_cn_variance_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    file_name = f'{dataset.name}.{workflow_name}_top_CN_variance.csv'

    return os.path.join(data_dir, file_name)


def get_dataset_spatial_top_cn_variance_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    file_name = f'{dataset.name}.{workflow_name}_top_CN_spatial_variance.csv'

    return os.path.join(data_dir, file_name)


def get_consensus_focal_gene_json_path(dataset_name):
    data_dir = os.path.join(GISTIC_HOME, 'consensus')
    file_name = f'{dataset_name}_focal_gene.json'

    return os.path.join(data_dir, file_name)


def get_consensus_gene_csv_path(dataset_name):
    data_dir = os.path.join(GISTIC_HOME, 'consensus')
    file_name = f'{dataset_name}_consensus_gene.csv'

    return os.path.join(data_dir, file_name)


def get_ora_csv_path(dataset_name, cn_type, workflow):
    if cn_type == 'consensus' and workflow == 'consensus':
        return os.path.join(GISTIC_HOME, 'consensus', f'{dataset_name}_consensus_term.csv')

    workflow_name = ora_workflow_map.get(workflow, workflow)
    folder_name = f'gistic_{dataset_name}.{workflow_name}'

    return os.path.join(GISTIC_HOME, cn_type_map[cn_type], folder_name, 'ora', 'focal_term.csv')
