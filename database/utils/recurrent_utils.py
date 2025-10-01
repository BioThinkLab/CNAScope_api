import os
import csv

from CNAScope_api.constant import GISTIC_HOME


def parse_recurrent_regions(filepath):
    try:
        with open(filepath, newline='', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter='\t')
            rows = list(reader)
    except FileNotFoundError:
        return None

    recurrent_regions_info = []
    col_num = len(rows[0]) if rows[0][-1] != '' else len(rows[0]) - 1
    for i in range(1, col_num):
        cytoband = rows[0][i]
        q_value = rows[1][i]
        boundaries = rows[3][i]

        genes = []

        for row in rows[4:]:
            gene = row[i]

            if gene:
                genes.append(gene)

        recurrent_regions_info.append({
            'cytoband': cytoband,
            'q_value': q_value,
            'boundaries': boundaries,
            'genes': genes
        })

    return recurrent_regions_info


def parse_recurrent_scores(filepath):
    try:
        with open(filepath, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            recurrent_scores_data = [row for row in reader]

            return recurrent_scores_data
    except FileNotFoundError:
        return None


def parse_recurrent_profiles(data):
    amp = []
    loss = []

    for region in data['bin']['amp']:
        amp.append(region)

    for region in data['bin']['loss']:
        loss.append(region)

    return {
        'amp': amp,
        'loss': loss,
        'cna': data['cna']
    }


workflow_map = {
    'ascat2': 'Ascat2',
    'ascat3': 'Ascat3',
    'ascatNGS': 'AscatNGS',
    'GATK4_CNV': 'GATK4 CNV',
    'DNAcopy': 'DNAcopy'
}


def has_gistic_fail(directory):
    return os.path.exists(os.path.join(directory, 'gistic.fail'))


def extract_workflow(sub_category):
    return sub_category.split('.')[-1]


def process_category(category, sub_category, dataset_name, result):
    if dataset_name in sub_category:
        sub_category_path = os.path.join(GISTIC_HOME, category, sub_category)

        if os.path.isdir(sub_category_path) and not has_gistic_fail(sub_category_path):
            workflow = workflow_map[extract_workflow(sub_category)]

            if category == 'allele-specific':
                result['allele'].append(workflow)
            elif category == 'copy-number-segment':
                result['cns'].append(workflow)
            elif category == 'masked-copy-number-segment':
                result['mcns'].append(workflow)


def get_gistic_options(dataset_name):
    result = {
        'allele': [],
        'cns': [],
        'mcns': []
    }

    categories = os.listdir(GISTIC_HOME)

    for category in categories:
        category_path = os.path.join(GISTIC_HOME, category)

        if category == 'consensus':
            continue

        if os.path.isdir(category_path):
            sub_categories = os.listdir(category_path)

            for sub_category in sub_categories:
                process_category(category, sub_category, dataset_name, result)

    result = {key: value for key, value in result.items() if value}

    return result


def has_ora(directory):
    return os.path.exists(os.path.join(directory, 'ora', 'ora_results.csv'))


def process_ora_category(category, sub_category, dataset_name, result):
    if dataset_name in sub_category:
        sub_category_path = os.path.join(GISTIC_HOME, category, sub_category)

        if os.path.isdir(sub_category_path) and has_ora(sub_category_path):
            workflow = workflow_map[extract_workflow(sub_category)]

            if category == 'allele-specific':
                result['allele'].append(workflow)
            elif category == 'copy-number-segment':
                result['cns'].append(workflow)
            elif category == 'masked-copy-number-segment':
                result['mcns'].append(workflow)


def get_ora_options(dataset_name):
    result = {
        'allele': [],
        'cns': [],
        'mcns': []
    }

    categories = os.listdir(GISTIC_HOME)

    for category in categories:
        category_path = os.path.join(GISTIC_HOME, category)

        if category == 'consensus':
            continue

        if os.path.isdir(category_path):
            sub_categories = os.listdir(category_path)

            for sub_category in sub_categories:
                process_category(category, sub_category, dataset_name, result)

    result = {key: value for key, value in result.items() if value}

    consensus_ora_name = f'{dataset_name}_consensus_term.csv'

    if os.path.exists(os.path.join(GISTIC_HOME, 'consensus', consensus_ora_name)):
        result['consensus'] = ['consensus']

    return result
