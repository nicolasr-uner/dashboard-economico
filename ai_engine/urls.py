from django.urls import path
from . import views

urlpatterns = [
    path('analyze/<int:variable_id>/', views.analyze_variable, name='analyze_variable'),
]