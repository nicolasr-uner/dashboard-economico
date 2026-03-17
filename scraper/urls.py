from django.urls import path
from . import views

urlpatterns = [
    path('test/', views.test_scraper, name='test_scraper'),
    path('run/<int:variable_id>/', views.run_scraper, name='run_scraper'),
    path('run-all/', views.run_all_scrapers, name='run_all_scrapers'),
]