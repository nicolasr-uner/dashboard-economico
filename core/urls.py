from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('configuracion/', views.settings_view, name='settings'),
    path('paises/', views.countries_view, name='countries'),
    path('variable/<int:variable_id>/', views.variable_detail, name='variable_detail'),
]