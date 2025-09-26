import os
import json

import pandas as pd
import typer


FILE_META_HOME = '/workspace2/zhengjieyi/CNVWebProject/scSVAS/combine_cnv_new/FileInfo'


def get_cnv_files_list_from_metadata(cnv_dir_path, project):
    metadata_path = os.path.join(FILE_META_HOME, project, 'metadata.json')

    with open(metadata_path, 'r') as f:
        metadata = json.load(f)

    dna_copy_files = []
    gatk_files = []

    for item in metadata:
        if item['analysis']['workflow_type'] == 'DNAcopy' and item['data_type'] == 'Masked Copy Number Segment':
            dna_copy_files.append(os.path.join(cnv_dir_path, item['file_id'], item['file_name']))

        if item['analysis']['workflow_type'] == 'GATK4 CNV' and item['data_type'] == 'Masked Copy Number Segment':
            gatk_files.append(os.path.join(cnv_dir_path, item['file_id'], item['file_name']))

    return dna_copy_files, gatk_files


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


def run_script(cnv_dir_path, output_dir_path):
    project = cnv_dir_path.split(os.sep)[-3]

    dna_copy_files, gatk_files = get_cnv_files_list_from_metadata(cnv_dir_path, project)

    print('OK')

    if len(dna_copy_files) != 0:
        dna_copy_df = get_cnv_df(project, 'DNAcopy', dna_copy_files)

        folder_name = f'gistic_{project}.DNAcopy'
        folder_path = os.path.join(output_dir_path, folder_name)

        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        else:
            print(f"Directory '{folder_path}' already exists.")

        dna_copy_df.to_csv(os.path.join(folder_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)

    if len(gatk_files) != 0:
        gatk_df = get_cnv_df(project, 'GATK4_CNV', gatk_files, is_gatk=True)

        folder_name = f'gistic_{project}.GATK4_CNV'
        folder_path = os.path.join(output_dir_path, folder_name)

        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        else:
            print(f"Directory '{folder_path}' already exists.")

        gatk_df.to_csv(os.path.join(folder_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)


def main(
    cnv_dir_path: str,
    output_dir_path: str,
):
    run_script(
        cnv_dir_path=cnv_dir_path,
        output_dir_path=output_dir_path,
    )


if __name__ == '__main__':
    typer.run(main)
