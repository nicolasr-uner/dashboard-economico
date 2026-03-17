import json
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.http import JsonResponse
from core.models import MacroVariable
from .analyzer import analyze_anomaly


@csrf_exempt
@require_http_methods(["POST"])
def analyze_variable(request, variable_id):
    try:
        variable = MacroVariable.objects.get(id=variable_id)
    except MacroVariable.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Variable no encontrada'})

    hist = variable.historical_data.order_by('-date')
    if hist.count() < 2:
        return JsonResponse({'success': False, 'error': 'Se necesitan al menos 2 datos históricos para analizar'})

    latest = hist[0]
    previous = hist[1]

    if previous.value != 0:
        detected_change = abs((latest.value - previous.value) / previous.value * 100)
    else:
        detected_change = 0.0

    result = analyze_anomaly(variable, detected_change, previous.value, latest.value)
    return JsonResponse(result)
