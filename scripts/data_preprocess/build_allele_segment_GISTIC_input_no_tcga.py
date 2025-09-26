import os
import json

import typer
import pandas as pd
import numpy as np


SAMPLE_META_HOME = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\Meta\\bulk_sample_meta'
FILE_META_HOME = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\FileInfo'
CNV_HOME = 'E:\\CNVWebProject\\CNVData\\GDC-V2'


def get_no_tcga_file_map():
    file_map = {}

    projects = ['TARGET-AML', 'TARGET-ALL-P2', 'MP2PRT-ALL', 'CPTAC-3', 'MMRF-COMMPASS', 'HCMI-CMDC', 'REBC-THYR',
                'TARGET-OS', 'CGCI-BLGSP', 'CGCI-HTMCP-CC', 'TARGET-ALL-P3', 'WCDT-MCRPC', 'APOLLO-LUAD',
                'CGCI-HTMCP-DLBCL', 'MP2PRT-WT', 'CDDP_EAGLE-1', 'CGCI-HTMCP-LC', 'TARGET-ALL-P1', 'TARGET-CCSK']

    for project in projects:
        with open(os.path.join(FILE_META_HOME, project, 'metadata.json'), 'r') as f:
            metadata = json.load(f)

        if project not in ['MMRF-COMMPASS', 'TARGET-ALL-P1', 'TARGET-ALL-P3', 'WCDT-MCRPC']:
            for item in metadata:
                if item['data_type'] == 'Allele-specific Copy Number Segment':
                    file_map[item['file_id']] = item['file_name']

        else:
            for item in metadata:
                if item['data_type'] == 'Copy Number Segment':
                    file_map[item['file_id']] = item['file_name']

    return file_map


def get_file_ids(path):
    df = pd.read_csv(path)
    sample_ids = df["sample_id"].tolist()

    return sample_ids


def get_no_tcga_file_dict():
    cptac = {}
    hcmi = {}
    gatk = {}
    other = {}

    for filename in os.listdir(SAMPLE_META_HOME):
        project = filename.split('.')[0]
        path = os.path.join(SAMPLE_META_HOME, filename)

        if filename.startswith('CPTAC'):
            cptac[project] = get_file_ids(path)
        elif filename.startswith('HCMI'):
            hcmi[project] = get_file_ids(path)
        elif project in ['MMRF-COMMPASS', 'TARGET-ALL-P1', 'TARGET-ALL-P3', 'WCDT-MCRPC']:
            gatk[project] = get_file_ids(path)
        elif not project.startswith('TCGA'):
            other[project] = get_file_ids(path)

    return {
        'cptac': cptac,
        'hcmi': hcmi,
        'gatk': gatk,
        'other': other
    }


def get_cnv_df(project, workflow, cnv_file_list, is_gatk=False):
    segment_df = None

    for file in cnv_file_list:
        file_id = os.path.basename(os.path.dirname(file))

        file_df = pd.read_csv(file, sep='\t')

        if is_gatk:
            file_df = file_df.drop(['GDC_Aliquot_ID', 'Num_Probes'], axis=1)
            file_df.insert(0, 'file_id', f'{project}.{workflow}_{file_id}')
            file_df.insert(4, 'Probe_Num', 1)
            file_df = file_df[file_df['Chromosome'] != 'chrM']
        else:
            file_df = file_df.drop(['GDC_Aliquot', 'Major_Copy_Number', 'Minor_Copy_Number'], axis=1)
            file_df.insert(0, 'file_id', f'{project}.{workflow}_{file_id}')
            file_df.insert(4, 'Probe_Num', 1)

        if segment_df is None:
            segment_df = file_df
        else:
            segment_df = pd.concat([segment_df, file_df], ignore_index=True)

    if is_gatk:
        segment_df['Chromosome'] = segment_df['Chromosome'].str.replace('chr', '', regex=False)
    else:
        segment_df['Chromosome'] = segment_df['Chromosome'].str.replace('chr', '', regex=False)
        segment_df['Copy_Number'] = segment_df['Copy_Number'].apply(
            lambda x: np.log2(x / 2.0) if x != 0 else np.log2(0.1 / 2)
        )

    return segment_df


def run_cptac(file_dict, file_map, output_dir_path):
    base_path = os.path.join(CNV_HOME, 'CPTAC-3', 'Copy_Number_Variation', 'Allele-specific_Copy_Number_Segment')
    for key, value in file_dict.items():
        files = [os.path.join(base_path, item, file_map[item]) for item in value]
        df = get_cnv_df(key, 'ascatNGS', files)

        output_path = os.path.join(output_dir_path, f'gistic_{key}.ascatNGS')
        os.makedirs(output_path, exist_ok=True)
        df.to_csv(os.path.join(output_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)


def run_hcmi(file_dict, file_map, output_dir_path):
    base_path = os.path.join(CNV_HOME, 'HCMI-CMDC', 'Copy_Number_Variation', 'Allele-specific_Copy_Number_Segment')
    for key, value in file_dict.items():
        files = [os.path.join(base_path, item, file_map[item]) for item in value]
        df = get_cnv_df(key, 'ascatNGS', files)

        output_path = os.path.join(output_dir_path, f'gistic_{key}.ascatNGS')
        os.makedirs(output_path, exist_ok=True)
        df.to_csv(os.path.join(output_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)


other_workflow_map = {
    'APOLLO-LUAD': 'ascatNGS',
    'CDDP_EAGLE-1': 'ascatNGS',
    'CGCI-BLGSP': 'ascatNGS',
    'CGCI-HTMCP-CC': 'ascatNGS',
    'CGCI-HTMCP-DLBCL': 'ascatNGS',
    'CGCI-HTMCP-LC': 'ascatNGS',
    'MP2PRT-ALL': 'ascatNGS',
    'MP2PRT-WT': 'ascatNGS',
    'REBC-THYR': 'ascatNGS',
    'TARGET-ALL-P2': 'ascat2',
    'TARGET-AML': 'ascat2',
    'TARGET-CCSK': 'ascat2',
    'TARGET-OS': 'ascat2',
}


def run_other(file_dict, file_map, output_dir_path):
    for key, value in file_dict.items():
        base_path = os.path.join(CNV_HOME, key, 'Copy_Number_Variation', 'Allele-specific_Copy_Number_Segment')
        files = [os.path.join(base_path, item, file_map[item]) for item in value]
        df = get_cnv_df(key, other_workflow_map[key], files)

        output_path = os.path.join(output_dir_path, f'gistic_{key}.{other_workflow_map[key]}')
        os.makedirs(output_path, exist_ok=True)
        df.to_csv(os.path.join(output_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)


def run_gatk(file_dict, file_map, output_dir_path):
    for key, value in file_dict.items():
        base_path = os.path.join(CNV_HOME, key, 'Copy_Number_Variation', 'Copy_Number_Segment')
        files = [os.path.join(base_path, item, file_map[item]) for item in value]
        df = get_cnv_df(key, 'GATK4_CNV', files, is_gatk=True)

        output_path = os.path.join(output_dir_path, f'gistic_{key}.GATK4_CNV')
        os.makedirs(output_path, exist_ok=True)
        df.to_csv(os.path.join(output_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)


def run_script(output_dir_path):
    file_map = get_no_tcga_file_map()
    file_dict = get_no_tcga_file_dict()

    run_cptac(file_dict['cptac'], file_map, output_dir_path)
    run_hcmi(file_dict['hcmi'], file_map, output_dir_path)
    run_other(file_dict['other'], file_map, output_dir_path)
    run_gatk(file_dict['gatk'], file_map, output_dir_path)


def main(output_dir_path: str):
    run_script(
        output_dir_path=output_dir_path,
    )


if __name__ == '__main__':
    typer.run(main)
