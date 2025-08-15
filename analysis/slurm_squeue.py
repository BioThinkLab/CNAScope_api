import subprocess
import os
from djang.conf import settings

sep = '/'

def squeue_by_job_name(uuid):
    command = [
        f"{settings.SLURM_SCRIPT_HOME}{sep}task_query.sh",
        uuid.replace('-', '_')
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        return result.stdout.strip()
    else:
        return False
