import os
import json


GDC_CASE_INFO_PATH = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\CaseInfo'


if __name__ == '__main__':
    dirs = os.listdir(GDC_CASE_INFO_PATH)
    case_num = 0

    for dir_name in dirs:
        meta_info_path = os.path.join(GDC_CASE_INFO_PATH, dir_name, 'cases.json')

        with open(meta_info_path, 'r') as f:
            meta_info = json.load(f)
            case_num += len(meta_info)

    print(f'GDC Case Num: {case_num}')
