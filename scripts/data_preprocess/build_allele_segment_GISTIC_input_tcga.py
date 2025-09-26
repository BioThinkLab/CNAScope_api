import os
import json

import pandas as pd
import numpy as np
import typer


FILE_META_HOME = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\FileInfo'


def get_cnv_files_list_from_metadata(cnv_dir_path, project):
    metadata_path = os.path.join(FILE_META_HOME, project, 'metadata.json')

    with open(metadata_path, 'r') as f:
        metadata = json.load(f)

    ascat2_files = []
    ascat3_files = []
    ascat_ngs_files = []

    for item in metadata:
        if item['analysis']['workflow_type'] == 'ASCAT2' and item['data_type'] == 'Allele-specific Copy Number Segment':
            ascat2_files.append(os.path.join(cnv_dir_path, item['file_id'], item['file_name']))

        if item['analysis']['workflow_type'] == 'ASCAT3' and item['data_type'] == 'Allele-specific Copy Number Segment':
            ascat3_files.append(os.path.join(cnv_dir_path, item['file_id'], item['file_name']))

        if (item['analysis']['workflow_type'] == 'AscatNGS' and
                item['data_type'] == 'Allele-specific Copy Number Segment'):
            ascat_ngs_files.append(os.path.join(cnv_dir_path, item['file_id'], item['file_name']))

    return ascat2_files, ascat3_files, ascat_ngs_files


def get_cnv_df(project, workflow, cnv_file_list):
    segment_df = None

    for file in cnv_file_list:
        file_id = os.path.basename(os.path.dirname(file))

        file_df = pd.read_csv(file, sep='\t')
        file_df = file_df.drop(['GDC_Aliquot', 'Major_Copy_Number', 'Minor_Copy_Number'], axis=1)
        file_df.insert(0, 'file_id', f'{project}.{workflow}_{file_id}')
        file_df.insert(4, 'Probe_Num', 1)

        if segment_df is None:
            segment_df = file_df
        else:
            segment_df = pd.concat([segment_df, file_df], ignore_index=True)

    segment_df['Chromosome'] = segment_df['Chromosome'].str.replace('chr', '', regex=False)
    segment_df['Copy_Number'] = segment_df['Copy_Number'].apply(
        lambda x: np.log2(x / 2.0) if x != 0 else np.log2(0.1 / 2)
    )

    return segment_df


def run_script(cnv_dir_path, output_dir_path):
    project = cnv_dir_path.split(os.sep)[-3]

    ascat2_files, ascat3_files, ascat_ngs_files = get_cnv_files_list_from_metadata(cnv_dir_path, project)

    if len(ascat2_files) != 0:
        ascat2_df = get_cnv_df(project, 'ascat2', ascat2_files)

        folder_name = f'gistic_{project}.ascat2'
        folder_path = os.path.join(output_dir_path, folder_name)

        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        else:
            print(f"Directory '{folder_path}' already exists.")

        ascat2_df.to_csv(os.path.join(folder_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)

    if len(ascat3_files) != 0:
        ascat3_df = get_cnv_df(project, 'ascat3', ascat3_files)

        folder_name = f'gistic_{project}.ascat3'
        folder_path = os.path.join(output_dir_path, folder_name)

        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        else:
            print(f"Directory '{folder_path}' already exists.")

        ascat3_df.to_csv(os.path.join(folder_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)

    if len(ascat_ngs_files) != 0:
        ascat_ngs_df = get_cnv_df(project, 'ascatNGS', ascat_ngs_files)

        folder_name = f'gistic_{project}.ascatNGS'
        folder_path = os.path.join(output_dir_path, folder_name)

        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        else:
            print(f"Directory '{folder_path}' already exists.")

        ascat_ngs_df.to_csv(os.path.join(folder_path, 'gistic_seg.txt'), index=False, sep='\t', header=False)



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
