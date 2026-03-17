from django.contrib import admin
from .models import Country, MacroVariable, HistoricalData, AIAnalysisLog


@admin.register(Country)
class CountryAdmin(admin.ModelAdmin):
    list_display = ['flag_emoji', 'name', 'code']
    search_fields = ['name', 'code']


@admin.register(MacroVariable)
class MacroVariableAdmin(admin.ModelAdmin):
    list_display = ['name', 'country', 'unit', 'frequency', 'is_dynamic', 'is_active', 'created_at']
    list_filter = ['country', 'frequency', 'is_active', 'is_dynamic']
    search_fields = ['name', 'country__name']
    list_editable = ['is_active']


@admin.register(HistoricalData)
class HistoricalDataAdmin(admin.ModelAdmin):
    list_display = ['variable', 'value', 'date', 'is_anomaly', 'scraped_at']
    list_filter = ['variable__country', 'is_anomaly']
    search_fields = ['variable__name']
    ordering = ['-date']


@admin.register(AIAnalysisLog)
class AIAnalysisLogAdmin(admin.ModelAdmin):
    list_display = ['variable', 'detected_change', 'ai_verdict', 'analyzed_at']
    list_filter = ['ai_verdict', 'variable__country']
    search_fields = ['variable__name']
    ordering = ['-analyzed_at']