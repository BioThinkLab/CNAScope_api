import os
import subprocess

import typer


GISTIC_RESULT_HOME = '/workspace2/zhengjieyi/GISTIC_HOME/GISTIC_Allele'


def main(project: str):
    ascat2_path = os.path.join(GISTIC_RESULT_HOME, f'gistic_{project}.ascat2')
    ascat3_path = os.path.join(GISTIC_RESULT_HOME, f'gistic_{project}.ascat3')
    ascatNGS_path = os.path.join(GISTIC_RESULT_HOME, f'gistic_{project}.ascatNGS')

    if os.path.exists(ascat2_path):
        segment_path = os.path.join(ascat2_path, 'gistic_seg.txt')
        subprocess.run([
            'bash',
            '/workspace2/zhengjieyi/GISTIC_HOME/GISTIC/run_gistic_script_without_marker.sh',
            ascat2_path,
            segment_path
        ])

    if os.path.exists(ascat3_path):
        segment_path = os.path.join(ascat3_path, 'gistic_seg.txt')
        subprocess.run([
            'bash',
            '/workspace2/zhengjieyi/GISTIC_HOME/GISTIC/run_gistic_script_without_marker.sh',
            ascat3_path,
            segment_path
        ])

    if os.path.exists(ascatNGS_path):
        segment_path = os.path.join(ascatNGS_path, 'gistic_seg.txt')
        subprocess.run([
            'bash',
            '/workspace2/zhengjieyi/GISTIC_HOME/GISTIC/run_gistic_script_without_marker.sh',
            ascatNGS_path,
            segment_path
        ])


if __name__ == '__main__':
    typer.run(main)
