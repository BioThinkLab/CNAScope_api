from django.db import models

# Create your models here.


class Dataset(models.Model):
    # Basic Dataset Information
    name = models.CharField(max_length=255, blank=True)
    full_name = models.TextField(blank=True)
    link = models.CharField(max_length=255, blank=True)

    # Dataset Source and Description
    source = models.CharField(max_length=255, blank=True)
    programme = models.CharField(max_length=255, blank=True)
    modality = models.CharField(max_length=255, blank=True)
    obs_type = models.CharField(max_length=255, blank=True)
    protocol = models.CharField(max_length=255, blank=True)
    platform = models.CharField(max_length=255, blank=True)
    workflow = models.CharField(max_length=255, blank=True)
    cn_type = models.CharField(max_length=255, blank=True)
    raw_cn_scale = models.CharField(max_length=255, blank=True)
    raw_locus_type = models.CharField(max_length=255, blank=True)
    reference = models.CharField(max_length=255, blank=True)

    # Clinical Information
    cancer_type = models.CharField(max_length=255, blank=True)
    cancer_type_full_name = models.TextField(blank=True)

    # Sample Information
    sample_num = models.PositiveIntegerField(blank=True, null=True)
    cell_num = models.PositiveIntegerField(blank=True, null=True)
    spot_num = models.PositiveIntegerField(blank=True, null=True)
    after_qc_ratio = models.FloatField(blank=True, null=True)
    usage_permission = models.CharField(max_length=255, blank=True)

    # Time Information
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


class BulkSampleMetadata(models.Model):
    # Basic Bulk Sample Information
    sample_id = models.CharField(max_length=255)
    dataset = models.ForeignKey(
        Dataset,
        on_delete=models.CASCADE,
        related_name='samples'
    )

    # Bulk Sample Clinical and Demographic Information
    disease_type = models.CharField(max_length=255, blank=True)
    primary_site = models.CharField(max_length=255, blank=True)
    tumor_stage = models.CharField(max_length=100, blank=True)
    tumor_grade = models.CharField(max_length=100, blank=True)
    ethnicity = models.CharField(max_length=255, blank=True)
    race = models.CharField(max_length=255, blank=True)
    gender = models.CharField(max_length=20, blank=True)
    age = models.IntegerField(blank=True, null=True)
    pfs = models.IntegerField(blank=True, null=True)
    days_to_death = models.IntegerField(blank=True, null=True)
    pfs_status = models.CharField(max_length=100, blank=True)
    vital_status = models.CharField(max_length=100, blank=True)

    # Time Information
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
