from django.urls import path, include
from .views import *
from . import visualization_views

urlpatterns = [
    path('submit_basic_annotation_task/', submit_basic_annotation_task, name='submit_basic_annotation_task'),
    path('submit_recurrent_cna_task/', submit_recurrent_cna_task, name='submit_recurrent_cna_task'),
    path('query_task/', query_task, name='query_task'),
    path('CNA_matrix/', visualization_views.CNAMatrixView.as_view(), name='CNA-matrix'),
    path('CNA_meta/', visualization_views.CNAMetaView.as_view(), name='CNA-meta'),
    path('CNA_tree/', visualization_views.CNATreeView.as_view(), name='CNA-tree'),
    path('CNA_genes/', visualization_views.CNAGeneListView.as_view(), name='CNA-gene-list'),
    path('CNA_newick/', visualization_views.CNANewickView.as_view(), name='CNA-newick'),
    path('CNA_gene_matrix/', visualization_views.CNAGeneMatrixView.as_view(), name='CNA-gene-matrix'),
    path('CNA_terms/', visualization_views.CNATermListView.as_view(), name='CNA-term-list'),
    path('CNA_term_matrix/', visualization_views.CNATermMatrixView.as_view(), name='CNA-term-matrix'),
    path('focal_CNA_info/', visualization_views.FocalCNAInfoView.as_view(), name='focal-CNA-info'),
    path('gene_recurrence_query/', visualization_views.GeneRecurrenceQueryView.as_view(), name='gene-recurrence-query'),
    path('ploidy_distribution/', visualization_views.PloidyDistributionView.as_view(), name='ploidy-distribution'),
    path('run_demo/', run_demo, name='run_demo'),
    path('download_task_data/', download_task_data, name='download_task_data'),

]
