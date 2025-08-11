from django.urls import path, include

from rest_framework.routers import DefaultRouter

from database.views import dataset_views, visualization_views

router = DefaultRouter()

router.register(r'datasets', dataset_views.DatasetListView, basename='dataset')

urlpatterns = [
    path('', include(router.urls)),
    path('bulk_samples/', dataset_views.BulkDatasetSampleListView.as_view(), name='bulk-sample-list'),
    path('CNA_matrix/', visualization_views.CNAMatrixView.as_view(), name='CNA-matrix'),
    path('CNA_meta/', visualization_views.CNAMetaView.as_view(), name='CNA-meta'),
    path('CNA_tree/', visualization_views.CNATreeView.as_view(), name='CNA-tree'),
    path('CNA_genes/', visualization_views.CNAGeneListView.as_view(), name='CNA-gene-list'),
    path('CNA_newick/', visualization_views.CNANewickView.as_view(), name='CNA-newick'),
    path('CNA_gene_matrix/', visualization_views.CNAGeneMatrixView.as_view(), name='CNA-gene-matrix'),
]
