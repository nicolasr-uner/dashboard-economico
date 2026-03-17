from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('core.urls')),
    path('api/scraper/', include('scraper.urls')),
    path('api/ai/', include('ai_engine.urls')),
]