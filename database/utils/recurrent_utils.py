import csv


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
