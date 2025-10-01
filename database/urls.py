from django.urls import path, include

from rest_framework.routers import DefaultRouter

from database.views import dataset_views, visualization_views

router = DefaultRouter()

router.register(r'datasets', dataset_views.DatasetListView, basename='dataset')

urlpatterns = [
    path('', include(router.urls)),
    path('samples/', dataset_views.DatasetSampleListView.as_view(), name='bulk-sample-list'),
    path('CNA_matrix/', visualization_views.CNAMatrixView.as_view(), name='CNA-matrix'),
    path('CNA_meta/', visualization_views.CNAMetaView.as_view(), name='CNA-meta'),
    path('CNA_tree/', visualization_views.CNATreeView.as_view(), name='CNA-tree'),
    path('CNA_genes/', visualization_views.CNAGeneListView.as_view(), name='CNA-gene-list'),
    path('CNA_newick/', visualization_views.CNANewickView.as_view(), name='CNA-newick'),
    path('CNA_gene_matrix/', visualization_views.CNAGeneMatrixView.as_view(), name='CNA-gene-matrix'),
    path('CNA_terms/', visualization_views.CNATermListView.as_view(), name='CNA-term-list'),
    path('CNA_term_matrix/', visualization_views.CNATermMatrixView.as_view(), name='CNA-term-matrix'),
    path('focal_CNA_options/', visualization_views.FocalCNAOptionsView.as_view(), name='CNA-options'),
    path('focal_CNA_info/', visualization_views.FocalCNAInfoView.as_view(), name='focal-CNA-info'),
    path('gene_recurrence_query/', visualization_views.GeneRecurrenceQueryView.as_view(), name='gene-recurrence-query'),
    path('ploidy_distribution/', visualization_views.PloidyDistributionView.as_view(), name='ploidy-distribution'),
    path('download_dataset/', dataset_views.download_dataset, name='download_dataset'),
    path('top_cn_variance/', visualization_views.TopCNVarianceView.as_view(), name='top-cn-variance'),
    path('CNA_vector/', visualization_views.CNAVectorView.as_view(), name='CNA-vector'),
    path('consensus_focal_gene/', visualization_views.CNAConsensusVennView.as_view(), name='consensus-focal-gene'),
    path('consensus_gene/', visualization_views.CNAConsensusGeneView.as_view(), name='consensus-gene'),
    path('consensus_gene_download/', visualization_views.CNAConsensusGeneDownloadView.as_view(),
         name='consensus-gene-download'),
    path('pathway_enrichment_options/', visualization_views.PathwayEnrichmentPlotOptionsView.as_view(), name='pathway-enrichment-options'),
    path('pathway_enrichment_plot/', visualization_views.PathwayEnrichmentPlotView.as_view(), name='pathway-enrichment-plot'),
]
