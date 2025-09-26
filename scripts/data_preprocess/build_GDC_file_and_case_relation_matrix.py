import os
import json
import pandas as pd


def verify_divided_project_result(info_base_dir, result_dir, project, prefix):
    file_info_path = os.path.join(info_base_dir, 'FileInfo', project, 'files.json')
    with open(file_info_path, 'r') as f:
        cptac_file_info = json.load(f)
    origin_num = len(cptac_file_info)

    cptac_file_case_matrix_list = []
    for sub_project in os.listdir(result_dir):
        if sub_project.startswith(prefix):
            cptac_file_case_matrix_list.append(sub_project.replace('.csv', ''))

    divided_num = 0
    for sub_project in cptac_file_case_matrix_list:
        df = pd.read_csv(os.path.join(result_dir, f'{sub_project}.csv'))
        divided_num += df.shape[0]

    print(project)
    print(f'Original num: {origin_num}')
    print(f'Divided num: {divided_num}')


def common_meta_matrix_process(project, file_case_id_df, case_info_dir, result_dir):
    case_info_path = os.path.join(case_info_dir, project, 'cases.json')
    with open(case_info_path, 'r') as f:
        case_info = json.load(f)

    case_name_id_list = []
    for info in case_info:
        case_name_id_list.append({
            'case_id': info['case_id'],
            'submitter_id': info['submitter_id']
        })

    case_name_id_df = pd.DataFrame(case_name_id_list)

    file_case_name_df = pd.merge(file_case_id_df, case_name_id_df, on='case_id', how='left')
    file_case_name_df = file_case_name_df.drop(columns=['case_id'])

    result_file_name = os.path.join(result_dir, f'{project}.csv')
    file_case_name_df.to_csv(result_file_name, index=False)


def divided_meta_matrix_process(file_case_id_df, result_dir, prefix):
    cptac_meta_matrix_dir = 'E:/CNVWebProject/CNVData/GDC-Info-V2/Meta/divided_project_info'

    cptac_project_list = []
    for project in os.listdir(cptac_meta_matrix_dir):
        if project.startswith(prefix):
            cptac_project_list.append(project)

    for project in cptac_project_list:
        case_info_path = os.path.join(cptac_meta_matrix_dir, project, 'cases.json')
        with open(case_info_path, 'r') as f:
            case_info = json.load(f)

        case_name_id_list = []
        for info in case_info:
            case_name_id_list.append({
                'case_id': info['case_id'],
                'submitter_id': info['submitter_id']
            })

        case_name_id_df = pd.DataFrame(case_name_id_list)

        file_case_name_df = pd.merge(file_case_id_df, case_name_id_df, on='case_id', how='left')
        file_case_name_df = file_case_name_df.dropna(subset=['submitter_id'])
        file_case_name_df = file_case_name_df.drop(columns=['case_id'])

        result_file_name = os.path.join(result_dir, f'{project}.csv')
        file_case_name_df.to_csv(result_file_name, index=False)


def extract_file_case_id_matrix(info_base_dir, result_dir):
    file_info_dir = os.path.join(info_base_dir, 'FileInfo')
    case_info_dir = os.path.join(info_base_dir, 'CaseInfo')

    project_list = []
    for project in os.listdir(file_info_dir):
        project_list.append(project)

    for project in project_list:
        file_info_path = os.path.join(file_info_dir, project, 'files.json')
        with open(file_info_path, 'r') as f:
            file_info = json.load(f)

        file_case_id_list = []
        for info in file_info:
            file_case_id_list.append({
                'file_id': info['file_id'],
                'case_id': info['cases'][0]['case_id']
            })

        file_case_id_df = pd.DataFrame(file_case_id_list)

        if project.startswith('CPTAC'):
            divided_meta_matrix_process(file_case_id_df, result_dir, 'CPTAC')
        elif project.startswith('HCMI'):
            divided_meta_matrix_process(file_case_id_df, result_dir, 'HCMI')
        else:
            common_meta_matrix_process(project, file_case_id_df, case_info_dir, result_dir)


if __name__ == '__main__':
    gdc_info_base_dir = 'E:/CNVWebProject/CNVData/GDC-Info-V2'
    output_dir = 'E:/CNVWebProject/CNVData/GDC-Info-V2/Meta/gdc_file_case_relation_matrix'

    extract_file_case_id_matrix(gdc_info_base_dir, output_dir)

    verify_divided_project_result(gdc_info_base_dir, output_dir, 'CPTAC-3', 'CPTAC')
    verify_divided_project_result(gdc_info_base_dir, output_dir, 'HCMI-CMDC', 'HCMI')
