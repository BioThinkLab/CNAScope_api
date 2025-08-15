# Create your models here.
from django.db import models
import uuid
import os
from django.conf import settings

class BasicAnnotationTask(models.Model):
    class Status(models.TextChoices):
        Success = 'S', 'Success',
        Pending = 'P', 'Pending',
        Failed = 'F', 'Failed',
        Running = 'R', 'Running',
    class Ref(models.TextChoices):
        hg19 = 'hg19'
        hg38 = 'hg38'
    class ObsType(models.TextChoices):
        bulk = 'sample', 'Bulk Sequencing',
        single = 'cell', 'Single-cell Sequencing',
        st = 'spot', 'Spatial Transcriptomics',
    class WindowType(models.TextChoices):
        bin = 'bin', 'Bin',
        gene = 'gene', 'Gene',
    class ValueType(models.TextChoices):
        int = 'int', 'Integer',
        log2 = 'log', 'Log',
    
    def get_input_file_path(instance, filename):
        return os.path.join(settings.WORKSPACE_HOME, str(instance.uuid), 'input', filename)
    def get_input_file_absolute_path(self):
        if self.input_file:
            return os.path.join(settings.WORKSPACE_HOME, str(self.uuid), 'input', os.path.basename(self.input_file.name))
        return None
    def get_output_dir_absolute_path(self):
        return os.path.join(settings.WORKSPACE_HOME, str(self.uuid), 'output')

    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.CharField(max_length=300, blank=True, null=True)
    status = models.CharField(choices=Status.choices, default=Status.Pending)
    create_time = models.DateTimeField(auto_now=False, auto_now_add=False)
    finish_time = models.DateTimeField(auto_now=False, auto_now_add=False, null=True, blank=True)
    k = models.IntegerField(default=10, blank=True, null=True)
    ref = models.CharField(choices=Ref.choices, default=Ref.hg38)
    obs_type = models.CharField(choices=ObsType.choices, default=ObsType.bulk)
    window_type = models.CharField(choices=WindowType.choices, default=WindowType.bin)
    value_type = models.CharField(choices=ValueType.choices, default=ValueType.int)
    input_file = models.FileField(upload_to=get_input_file_path, null=True, blank=True)

class RecurrentCNATask(models.Model):
    class Status(models.TextChoices):
        Success = 'S', 'Success',
        Pending = 'P', 'Pending',
        Failed = 'F', 'Failed',
        Running = 'R', 'Running',
    class Ref(models.TextChoices):
        hg19 = 'hg19'
        hg38 = 'hg38'
    class ObsType(models.TextChoices):
        bulk = 'sample', 'Bulk Sequencing',
        single = 'cell', 'Single-cell Sequencing',
        st = 'spot', 'Spatial Transcriptomics',
    class ValueType(models.TextChoices):
        int = 'int', 'Integer',
        log2 = 'log', 'Log',
    
    def get_input_file_path(instance, filename):
        return os.path.join(settings.WORKSPACE_HOME, str(instance.uuid), 'input', filename)
    def get_input_file_absolute_path(self):
        if self.input_file:
            return os.path.join(settings.WORKSPACE_HOME, str(self.uuid), 'input', os.path.basename(self.input_file.name))
        return None
    def get_output_dir_absolute_path(self):
        return os.path.join(settings.WORKSPACE_HOME, str(self.uuid), 'output')

    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.CharField(max_length=300, blank=True, null=True)
    status = models.CharField(choices=Status.choices, default=Status.Pending)
    create_time = models.DateTimeField(auto_now=False, auto_now_add=False)
    finish_time = models.DateTimeField(auto_now=False, auto_now_add=False, null=True, blank=True)
    ref = models.CharField(choices=Ref.choices, default=Ref.hg38)
    obs_type = models.CharField(choices=ObsType.choices, default=ObsType.bulk)
    value_type = models.CharField(choices=ValueType.choices, default=ValueType.int)
    input_file = models.FileField(upload_to=get_input_file_path, null=True, blank=True)
