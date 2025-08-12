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
    'GATK4 CNV': 'GATK4_CNV'
}


def build_dataset_data_dir_path(dataset):
    return os.path.join(DATA_HOME, dataset.modality, source_map[dataset.source])


def get_dataset_matrix_path(dataset, workflow):
    data_dir = str(build_dataset_data_dir_path(dataset))
    workflow_name = workflow_map.get(workflow, workflow)
    matrix_name = f'{dataset.name}.{workflow_name}.CNA.csv'

    return os.path.join(data_dir, 'clean', matrix_name)


def get_dataset_meta_path(dataset, workflow):
    data_dir = str(build_dataset_data_dir_path(dataset))
    workflow_name = workflow_map.get(workflow, workflow)
    meta_name = f'{dataset.name}.{workflow_name}_meta_scsvas.csv'

    return os.path.join(data_dir, 'out', meta_name)


def get_dataset_tree_path(dataset, workflow):
    data_dir = str(build_dataset_data_dir_path(dataset))
    workflow_name = workflow_map.get(workflow, workflow)
    tree_name = f'{dataset.name}.{workflow_name}_cut64.json'

    return os.path.join(data_dir, 'out', tree_name)


def get_dataset_gene_matrix_path(dataset, workflow):
    data_dir = str(build_dataset_data_dir_path(dataset))
    workflow_name = workflow_map.get(workflow, workflow)
    gene_matrix_name = f'{dataset.name}.{workflow_name}_gene_cna.parquet'

    return os.path.join(data_dir, 'out', gene_matrix_name)


def get_dataset_newick_path(dataset, workflow):
    data_dir = str(build_dataset_data_dir_path(dataset))
    workflow_name = workflow_map.get(workflow, workflow)
    newick_name = f'{dataset.name}.{workflow_name}.nwk'

    return os.path.join(data_dir, 'out', newick_name)


def get_dataset_term_matrix_path(dataset, workflow):
    data_dir = str(build_dataset_data_dir_path(dataset))
    workflow_name = workflow_map.get(workflow, workflow)
    term_matrix_name = f'{dataset.name}.{workflow_name}_term_cna.parquet'

    return os.path.join(data_dir, 'out', term_matrix_name)
