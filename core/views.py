import json
from datetime import date, timedelta
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib import messages
from django.db.models import Count, Q
from django.utils import timezone
from .models import Country, MacroVariable, HistoricalData, AIAnalysisLog
from .forms import CountryForm, MacroVariableForm


def dashboard(request):
    countries = Country.objects.prefetch_related(
        'variables__historical_data'
    ).all()

    recent_alerts = AIAnalysisLog.objects.select_related(
        'variable__country'
    ).order_by('-analyzed_at')[:5]

    total_variables = MacroVariable.objects.filter(is_active=True).count()
    total_countries = Country.objects.count()

    thirty_days_ago = timezone.now().date() - timedelta(days=30)
    anomalies_this_month = HistoricalData.objects.filter(
        is_anomaly=True,
        date__gte=thirty_days_ago
    ).count()

    # Chart data: IPC de los últimos 6 meses por país
    chart_data = {}
    six_months_ago = date.today() - timedelta(days=180)

    for country in countries:
        ipc_var = country.variables.filter(
            name__icontains='IPC'
        ).first()
        if not ipc_var:
            ipc_var = country.variables.filter(
                name__icontains='IPCA'
            ).first()

        if ipc_var:
            hist = ipc_var.historical_data.filter(
                date__gte=six_months_ago
            ).order_by('date')

            chart_data[country.name] = {
                'labels': [str(h.date) for h in hist],
                'data': [h.value for h in hist],
                'flag': country.flag_emoji,
            }

    # All variables with latest data
    variables_with_data = []
    for country in countries:
        for var in country.variables.filter(is_active=True):
            latest_hist = var.historical_data.order_by('-date').first()
            prev_hist = var.historical_data.order_by('-date')[1:2].first()

            change = None
            if latest_hist and prev_hist and prev_hist.value != 0:
                change = round(((latest_hist.value - prev_hist.value) / prev_hist.value) * 100, 2)

            variables_with_data.append({
                'variable': var,
                'country': country,
                'latest': latest_hist,
                'change': change,
            })

    context = {
        'countries': countries,
        'recent_alerts': recent_alerts,
        'total_variables': total_variables,
        'total_countries': total_countries,
        'anomalies_this_month': anomalies_this_month,
        'chart_data': json.dumps(chart_data),
        'variables_with_data': variables_with_data,
    }
    return render(request, 'dashboard/index.html', context)


def settings_view(request):
    country_form = CountryForm()
    variable_form = MacroVariableForm()

    if request.method == 'POST':
        if 'save_country' in request.POST:
            country_form = CountryForm(request.POST)
            if country_form.is_valid():
                country_form.save()
                messages.success(request, 'País guardado exitosamente.')
                return redirect('settings')
            else:
                messages.error(request, 'Error al guardar el país.')
        elif 'save_variable' in request.POST:
            variable_form = MacroVariableForm(request.POST)
            if variable_form.is_valid():
                variable_form.save()
                messages.success(request, 'Variable guardada exitosamente.')
                return redirect('settings')
            else:
                messages.error(request, 'Error al guardar la variable.')

    countries = Country.objects.all()
    variables = MacroVariable.objects.select_related('country').all()

    context = {
        'country_form': country_form,
        'variable_form': variable_form,
        'countries': countries,
        'variables': variables,
    }
    return render(request, 'dashboard/settings.html', context)


def countries_view(request):
    countries = Country.objects.prefetch_related(
        'variables__historical_data'
    ).all()

    countries_data = []
    for country in countries:
        vars_data = []
        for var in country.variables.filter(is_active=True):
            latest = var.historical_data.order_by('-date').first()
            prev = var.historical_data.order_by('-date')[1:2].first()
            change = None
            if latest and prev and prev.value != 0:
                change = round(((latest.value - prev.value) / prev.value) * 100, 2)
            vars_data.append({'var': var, 'latest': latest, 'change': change})
        countries_data.append({'country': country, 'variables': vars_data})

    return render(request, 'dashboard/countries.html', {'countries_data': countries_data})


def variable_detail(request, variable_id):
    variable = get_object_or_404(MacroVariable, id=variable_id)
    historical = variable.historical_data.order_by('date')
    ai_logs = variable.ai_logs.order_by('-analyzed_at')[:10]

    chart_labels = [str(h.date) for h in historical]
    chart_values = [h.value for h in historical]

    context = {
        'variable': variable,
        'historical': historical,
        'ai_logs': ai_logs,
        'chart_labels': json.dumps(chart_labels),
        'chart_values': json.dumps(chart_values),
    }
    return render(request, 'dashboard/variable_detail.html', context)