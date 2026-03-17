from django.db import models
from django.utils import timezone


class Country(models.Model):
    name = models.CharField(max_length=100, verbose_name="Nombre")
    code = models.CharField(max_length=5, unique=True, verbose_name="Código ISO")
    flag_emoji = models.CharField(max_length=10, verbose_name="Bandera")

    class Meta:
        verbose_name = "País"
        verbose_name_plural = "Países"
        ordering = ['name']

    def __str__(self):
        return f"{self.flag_emoji} {self.name}"


class MacroVariable(models.Model):
    FREQUENCY_CHOICES = [
        ('daily', 'Diario'),
        ('weekly', 'Semanal'),
        ('monthly', 'Mensual'),
        ('quarterly', 'Trimestral'),
    ]

    country = models.ForeignKey(Country, on_delete=models.CASCADE, related_name='variables', verbose_name="País")
    name = models.CharField(max_length=200, verbose_name="Nombre")
    description = models.TextField(blank=True, verbose_name="Descripción")
    source_url = models.URLField(blank=True, verbose_name="URL fuente")
    css_selector = models.CharField(max_length=500, blank=True, verbose_name="Selector CSS")
    frequency = models.CharField(max_length=20, choices=FREQUENCY_CHOICES, default='monthly', verbose_name="Frecuencia")
    is_dynamic = models.BooleanField(default=False, verbose_name="Scraping dinámico")
    unit = models.CharField(max_length=50, verbose_name="Unidad")
    is_active = models.BooleanField(default=True, verbose_name="Activa")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Variable Macroeconómica"
        verbose_name_plural = "Variables Macroeconómicas"
        ordering = ['country', 'name']

    def __str__(self):
        return f"{self.country.code} - {self.name}"

    def latest_value(self):
        last = self.historical_data.order_by('-date').first()
        return last.value if last else None

    def previous_value(self):
        data = self.historical_data.order_by('-date')
        if data.count() >= 2:
            return data[1].value
        return None

    def monthly_change(self):
        latest = self.latest_value()
        previous = self.previous_value()
        if latest is not None and previous is not None and previous != 0:
            return round(((latest - previous) / previous) * 100, 2)
        return None


class HistoricalData(models.Model):
    variable = models.ForeignKey(MacroVariable, on_delete=models.CASCADE, related_name='historical_data', verbose_name="Variable")
    value = models.FloatField(verbose_name="Valor")
    date = models.DateField(verbose_name="Fecha")
    is_anomaly = models.BooleanField(default=False, verbose_name="¿Anomalía?")
    scraped_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Dato Histórico"
        verbose_name_plural = "Datos Históricos"
        ordering = ['-date']
        unique_together = [['variable', 'date']]

    def __str__(self):
        return f"{self.variable.name} - {self.date}: {self.value}"


class AIAnalysisLog(models.Model):
    VERDICT_CHOICES = [
        ('transitorio', 'Transitorio'),
        ('estructural', 'Estructural'),
        ('indeterminado', 'Indeterminado'),
    ]

    variable = models.ForeignKey(MacroVariable, on_delete=models.CASCADE, related_name='ai_logs', verbose_name="Variable")
    detected_change = models.FloatField(verbose_name="Cambio detectado (%)")
    ai_verdict = models.CharField(max_length=20, choices=VERDICT_CHOICES, default='indeterminado', verbose_name="Veredicto IA")
    justification = models.TextField(verbose_name="Justificación")
    news_context = models.TextField(blank=True, verbose_name="Contexto de noticias")
    analyzed_at = models.DateTimeField(auto_now_add=True)
    risk_level = models.CharField(max_length=10, default='medio', verbose_name="Nivel de riesgo")
    recommendation = models.TextField(blank=True, verbose_name="Recomendación")

    class Meta:
        verbose_name = "Análisis IA"
        verbose_name_plural = "Análisis IA"
        ordering = ['-analyzed_at']

    def __str__(self):
        return f"{self.variable.name} - {self.ai_verdict} ({self.analyzed_at.date()})"