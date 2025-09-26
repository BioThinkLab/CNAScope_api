import os
import json

import pandas as pd

CASE_META_HOME = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\CaseInfo'
OUTPUT_HOME = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\Meta\\divided_project_case_map'


if __name__ == '__main__':
    cptac_file_meta_path = os.path.join(CASE_META_HOME, 'CPTAC-3', 'cases.json')
    hcmi_file_meta_path = os.path.join(CASE_META_HOME, 'HCMI-CMDC', 'cases.json')

    with open(cptac_file_meta_path, 'r') as f:
        cptac_file_meta = json.load(f)

    cptac_case_id_map = {}

    for item in cptac_file_meta:
        cptac_case_id_map[item['submitter_id']] = item['case_id']

    with open(hcmi_file_meta_path, 'r') as f:
        hcmi_file_meta = json.load(f)

    hcmi_case_id_map = {}

    for item in hcmi_file_meta:
        hcmi_case_id_map[item['submitter_id']] = item['case_id']

    with open(os.path.join(OUTPUT_HOME, 'cptac.json'), 'w') as f:
        json.dump(cptac_case_id_map, f)

    with open(os.path.join(OUTPUT_HOME, 'hcmi.json'), 'w') as f:
        json.dump(hcmi_case_id_map, f)
