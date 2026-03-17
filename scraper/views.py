import json
from datetime import date
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.http import JsonResponse
from django.utils import timezone
from core.models import MacroVariable, HistoricalData
from .engine import scrape
from ai_engine.analyzer import analyze_anomaly


@csrf_exempt
@require_http_methods(["POST"])
def test_scraper(request):
    try:
        data = json.loads(request.body)
        url = data.get('url', '')
        css_selector = data.get('css_selector', '')
        is_dynamic = data.get('is_dynamic', False)

        if not url or not css_selector:
            return JsonResponse({'success': False, 'error': 'URL y selector CSS son requeridos'})

        result = scrape(url, css_selector, is_dynamic)
        return JsonResponse(result)

    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'error': 'JSON inválido'})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})


@csrf_exempt
@require_http_methods(["POST"])
def run_scraper(request, variable_id):
    try:
        variable = MacroVariable.objects.get(id=variable_id, is_active=True)
    except MacroVariable.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Variable no encontrada'})

    if not variable.source_url or not variable.css_selector:
        return JsonResponse({'success': False, 'error': 'Variable sin URL o selector CSS configurado'})

    result = scrape(variable.source_url, variable.css_selector, variable.is_dynamic)

    if not result['success']:
        return JsonResponse({'success': False, 'error': result['error']})

    new_value = result['value']
    today = date.today()

    # Obtener valor previo para detectar anomalías
    prev = variable.historical_data.order_by('-date').first()
    is_anomaly = False
    detected_change = 0.0

    if prev and prev.value != 0:
        detected_change = abs((new_value - prev.value) / prev.value * 100)
        is_anomaly = detected_change > 8.0

    hist, created = HistoricalData.objects.update_or_create(
        variable=variable,
        date=today,
        defaults={
            'value': new_value,
            'is_anomaly': is_anomaly,
            'scraped_at': timezone.now(),
        }
    )

    response_data = {
        'success': True,
        'variable': variable.name,
        'value': new_value,
        'date': str(today),
        'is_anomaly': is_anomaly,
        'detected_change': round(detected_change, 2),
        'created': created,
    }

    # Disparar análisis IA si es anomalía
    if is_anomaly and prev:
        try:
            ai_result = analyze_anomaly(variable, detected_change, prev.value, new_value)
            response_data['ai_analysis'] = ai_result
        except Exception as e:
            response_data['ai_analysis'] = {'error': str(e)}

    return JsonResponse(response_data)


@csrf_exempt
@require_http_methods(["POST"])
def run_all_scrapers(request):
    variables = MacroVariable.objects.filter(is_active=True, source_url__isnull=False).exclude(source_url='')

    results = []
    ok_count = 0
    error_count = 0

    for variable in variables:
        if not variable.css_selector:
            results.append({'variable': variable.name, 'success': False, 'error': 'Sin selector CSS'})
            error_count += 1
            continue

        result = scrape(variable.source_url, variable.css_selector, variable.is_dynamic)

        if result['success']:
            ok_count += 1
            results.append({'variable': variable.name, 'success': True, 'value': result['value']})
        else:
            error_count += 1
            results.append({'variable': variable.name, 'success': False, 'error': result['error']})

    return JsonResponse({
        'total': len(results),
        'ok': ok_count,
        'errors': error_count,
        'results': results,
    })