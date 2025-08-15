from django.urls import path, include
from .views import *

urlpatterns = [
    path('submit_basic_annotation_task/', submit_basic_annotation_task, name='submit_basic_annotation_task'),
    path('submit_recurrent_cna_task/', submit_recurrent_cna_task, name='submit_recurrent_cna_task'),
    path('query_task/', query_task, name='query_task'),

]
