import os

from CNAScope_api.constant import DATA_HOME


source_map = {
    'cBioportal': 'cBioPortal',
    'COSMIC': 'COSMIC',
    'GDC Portal': 'GDC',
    '10x Official': '10x',
    'HSCGD': 'HSCGD',
    'scTML': 'scTML'
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
    'NA': 'nan'
}


def build_dataset_data_dir_path(dataset):
    return os.path.join(DATA_HOME, dataset.modality, source_map[dataset.source])


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
    tree_cut_map = {
        '5M': 50,
        '200kb': 5,
        '500kb': 15
    }
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
        cut_suffix = f'cut{tree_cut_map[bin_size]}'
    else:
        data_dir = os.path.join(data_base_dir, 'out')
        cut_suffix = f'cut64'

    workflow_name = workflow_map.get(workflow, workflow)
    tree_name = f'{dataset.name}.{workflow_name}_{cut_suffix}.json'

    return os.path.join(data_dir, tree_name)


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


def get_dataset_recurrent_scores_path(dataset, workflow, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
    folder_name = f'gistic_{dataset.name}.{workflow_name}'

    return os.path.join(data_dir, folder_name, 'scores.gistic')


def get_dataset_recurrent_gene_path(dataset, workflow, recurrent_type, bin_size):
    data_base_dir = str(build_dataset_data_dir_path(dataset))

    if dataset.source == 'GDC Portal':
        data_dir = os.path.join(data_base_dir, 'out', bin_size)
    else:
        data_dir = os.path.join(data_base_dir, 'out')

    workflow_name = workflow_map.get(workflow, workflow)
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
