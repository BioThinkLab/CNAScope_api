import subprocess
import os
from .models import *
from django.conf import settings

seperator = '/'

def sbatch_basic_annotation_task(uuid):
    task = BasicAnnotationTask.objects.get(uuid=uuid)
    output_dir = task.get_output_dir_absolute_path()
    command = [
        "sbatch",
        f"--job-name={str(uuid).replace('-', '_')}",
        f"--output={output_dir}{seperator}Pipeline.out",
        f"--error={output_dir}{seperator}Pipeline.err",
        f"{settings.SLURM_SCRIPT_HOME}{seperator}run_basic_cna_anno.sh",
        str(task.uuid),
        task.get_input_file_absolute_path(),
        task.ref,
        task.obs_type,
        task.window_type,
        task.k,
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        return True
    else:
        return False


def sbatch_recurrent_cna_task(uuid, input_files):
    task = RecurrentCNATask.objects.get(uuid=uuid)
    output_dir = task.get_output_dir_absolute_path()
    command = [
        "sbatch",
        f"--job-name={str(uuid).replace('-', '_')}",
        f"--output={output_dir}{seperator}Pipeline.out",
        f"--error={output_dir}{seperator}Pipeline.err",
        f"{settings.SLURM_SCRIPT_HOME}{seperator}run_recurrent_cna_task.sh",
        str(task.uuid),
        input_files,
        task.ref,
        task.obs_type,
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        return True
    else:
        return False