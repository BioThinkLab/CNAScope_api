import os
import json


GDC_FILE_INFO_PATH = 'E:\\CNVWebProject\\CNVData\\GDC-Info-V2\\FileInfo'


if __name__ == '__main__':
    dirs = os.listdir(GDC_FILE_INFO_PATH)
    case_num = 0
    allele_num = 0
    cns_num = 0
    mcns_num = 0
    gcns_num = 0

    for dir_name in dirs:
        meta_info_path = os.path.join(GDC_FILE_INFO_PATH, dir_name, 'metadata.json')
        file_info_path = os.path.join(GDC_FILE_INFO_PATH, dir_name, 'files.json')

        with open(meta_info_path, 'r') as f:
            meta_info = json.load(f)
            case_num += len(meta_info)

        with open(file_info_path, 'r') as f:
            file_info = json.load(f)

            allele_num += len([item for item in file_info if item['data_type'] == 'Allele-specific Copy Number Segment'])
            cns_num += len([item for item in file_info if item['data_type'] == 'Copy Number Segment'])
            mcns_num += len([item for item in file_info if item['data_type'] == 'Masked Copy Number Segment'])
            gcns_num += len([item for item in file_info if item['data_type'] == 'Gene Level Copy Number'])

    print(f'GDC Case Num: {case_num}')
    print(f'GDC Allele Num: {allele_num}')
    print(f'GDC CNS Num: {cns_num}')
    print(f'GDC MCNS Num: {mcns_num}')
    print(f'GDC GCNS Num: {gcns_num}')
