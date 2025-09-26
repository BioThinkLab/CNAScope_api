import os
import json

import typer
import pandas as pd
import numpy as np

# CASE_META_HOME = '/workspace2/zhengjieyi/CNVWebProject/scSVAS/cns_gistic/bulk_case_meta'
CASE_META_HOME = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\Meta\\bulk_case_meta'
# FILE_META_HOME = '/workspace2/zhengjieyi/CNVWebProject/scSVAS/combine_cnv_new/FileInfo'
FILE_META_HOME = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\FileInfo'
CNV_HOME = '/workspace/pengsisi/GDC/GDC-CNV-Data-V2'
# ID_MAP_HOME = '/workspace2/zhengjieyi/CNVWebProject/scSVAS/cns_gistic/divided_project_case_map'
ID_MAP_HOME = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\Meta\\divided_project_case_map'


def get_no_tcga_file_map():
    file_map = {}

    projects = ['CPTAC-3', 'HCMI-CMDC']

    for project in projects:
        with open(os.path.join(FILE_META_HOME, project, 'metadata.json'), 'r') as f:
            metadata = json.load(f)

        for item in metadata:
            if item['data_type'] == 'Masked Copy Number Segment':
                file_map[item['file_id']] = item['file_name']

    return file_map


def get_file_ids(path, is_cptac=True):
    df = pd.read_csv(path)
    sample_ids = df["sample_id"].tolist()
    file_ids = []

    if is_cptac:
        metadata_path = os.path.join(FILE_META_HOME, 'CPTAC-3', 'metadata.json')
        id_map_path = os.path.join(ID_MAP_HOME, 'cptac.json')
    else:
        metadata_path = os.path.join(FILE_META_HOME, 'HCMI-CMDC', 'metadata.json')
        id_map_path = os.path.join(ID_MAP_HOME, 'hcmi.json')

    with open(metadata_path, 'r') as f:
        metadata = json.load(f)

    with open(id_map_path, 'r') as f:
        id_map = json.load(f)

    case_ids = [id_map[sample_id] for sample_id in sample_ids]

    for meta in metadata:
        if meta['associated_entities'][0]['case_id'] in case_ids and meta['data_type'] == 'Masked Copy Number Segment':
            file_ids.append(meta['file_id'])

    return file_ids


def get_no_tcga_file_dict():
    cptac = {}
    hcmi = {}

    for filename in os.listdir(CASE_META_HOME):
        if filename == 'HCMI-CMDC-HNSC.meta.csv':
            continue

        project = filename.split('.')[0]
        path = os.path.join(CASE_META_HOME, filename)

        if filename.startswith('CPTAC'):
            cptac[project] = get_file_ids(path)
        elif filename.startswith('HCMI'):
            hcmi[project] = get_file_ids(path, is_cptac=False)

    return {
        'cptac': cptac,
        'hcmi': hcmi
    }


def get_cnv_df(project, workflow, cnv_file_list, is_gatk=False):
    segment_df = None

    for file in cnv_file_list:
        file_id = os.path.basename(os.path.dirname(file))

        file_df = pd.read_csv(file, sep='\t')
        if is_gatk:
            file_df = file_df.drop(['GDC_Aliquot_ID'], axis=1)
            file_df = file_df[file_df['Chromosome'] != 'chrM']
        else:
            file_df = file_df.drop(['GDC_Aliquot'], axis=1)
        file_df.insert(0, 'file_id', f'{project}.{workflow}_{file_id}')

        if segment_df is None:
            segment_df = file_df
        else:
            segment_df = pd.concat([segment_df, file_df], ignore_index=True)

    if is_gatk:
        segment_df['Chromosome'] = segment_df['Chromosome'].str.replace('chr', '', regex=False)

    return segment_df


def run_cptac(file_dict, file_map, output_dir_path):
    base_path = os.path.join(CNV_HOME, 'CPTAC-3', 'Copy_Number_Variation', 'Copy_Number_Segment')
    for key, value in file_dict.items():
        files = [os.path.join(base_path, item, file_map[item]) for item in value]
        df = get_cnv_df(key, 'GATK4_CNV', files, is_gatk=True)

        output_path = os.path.join(output_dir_path, f'gistic_{key}.GATK4_CNV')
        os.makedirs(output_path, exist_ok=True)
        df.to_csv(os.path.join(output_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)


def run_hcmi(file_dict, file_map, output_dir_path):
    base_path = os.path.join(CNV_HOME, 'HCMI-CMDC', 'Copy_Number_Variation', 'Copy_Number_Segment')
    for key, value in file_dict.items():
        files = [os.path.join(base_path, item, file_map[item]) for item in value]
        df = get_cnv_df(key, 'GATK4_CNV', files, is_gatk=True)

        output_path = os.path.join(output_dir_path, f'gistic_{key}.GATK4_CNV')
        os.makedirs(output_path, exist_ok=True)
        df.to_csv(os.path.join(output_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)


def run_script(output_dir_path):
    file_map = get_no_tcga_file_map()
    file_dict = get_no_tcga_file_dict()

    print('OK')

    run_cptac(file_dict['cptac'], file_map, output_dir_path)
    run_hcmi(file_dict['hcmi'], file_map, output_dir_path)


def main(output_dir_path: str):
    run_script(
        output_dir_path=output_dir_path,
    )


if __name__ == '__main__':
    typer.run(main)
