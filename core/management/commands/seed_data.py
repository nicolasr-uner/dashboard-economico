import random
from datetime import date, timedelta
from django.core.management.base import BaseCommand
from core.models import Country, MacroVariable, HistoricalData


def gen_series(base, variation, months=12, min_val=None, max_val=None):
    values = []
    current = base
    for _ in range(months):
        change = random.uniform(-variation, variation)
        current = current + change
        if min_val is not None:
            current = max(current, min_val)
        if max_val is not None:
            current = min(current, max_val)
        values.append(round(current, 2))
    return values


class Command(BaseCommand):
    help = 'Carga datos precargados de paises y variables macroeconomicas'

    def handle(self, *args, **options):
        self.stdout.write('Cargando datos precargados...')

        countries_data = [
            ('Colombia', 'CO', '\U0001f1e8\U0001f1f4'),
            ('Mexico', 'MX', '\U0001f1f2\U0001f1fd'),
            ('Ecuador', 'EC', '\U0001f1ea\U0001f1e8'),
            ('Brasil', 'BR', '\U0001f1e7\U0001f1f7'),
        ]

        countries = {}
        for name, code, flag in countries_data:
            country, created = Country.objects.get_or_create(
                code=code,
                defaults={'name': name, 'flag_emoji': flag}
            )
            countries[code] = country
            status = 'creado' if created else 'existente'
            self.stdout.write(f'  Pais {status}: {name} ({code})')

        variables_config = {
            'CO': [
                {
                    'name': 'IPC (Inflacion Mensual)',
                    'description': 'Indice de Precios al Consumidor mensual de Colombia',
                    'unit': '%',
                    'frequency': 'monthly',
                    'base': 7.5, 'variation': 0.5, 'min_val': 5.5, 'max_val': 9.5,
                },
                {
                    'name': 'TRM (USD/COP)',
                    'description': 'Tasa Representativa del Mercado - pesos colombianos por dolar',
                    'unit': 'COP',
                    'frequency': 'daily',
                    'base': 4100, 'variation': 80, 'min_val': 3900, 'max_val': 4400,
                },
                {
                    'name': 'Precio Energia Mayorista (Bolsa)',
                    'description': 'Precio de la energia electrica en bolsa colombiana',
                    'unit': 'COP/kWh',
                    'frequency': 'daily',
                    'base': 350, 'variation': 60, 'min_val': 180, 'max_val': 650,
                },
                {
                    'name': 'Tasa de Desempleo',
                    'description': 'Tasa de desempleo nacional de Colombia',
                    'unit': '%',
                    'frequency': 'monthly',
                    'base': 10.5, 'variation': 0.4, 'min_val': 9.5, 'max_val': 12.5,
                },
            ],
            'MX': [
                {
                    'name': 'IPC (Inflacion Mensual)',
                    'description': 'Indice de Precios al Consumidor mensual de Mexico',
                    'unit': '%',
                    'frequency': 'monthly',
                    'base': 6.0, 'variation': 0.5, 'min_val': 4.2, 'max_val': 8.1,
                },
                {
                    'name': 'Tipo de Cambio USD/MXN',
                    'description': 'Pesos mexicanos por dolar estadounidense',
                    'unit': 'MXN',
                    'frequency': 'daily',
                    'base': 18.0, 'variation': 0.5, 'min_val': 16.5, 'max_val': 19.8,
                },
                {
                    'name': 'Tasa de Desempleo',
                    'description': 'Tasa de desempleo nacional de Mexico',
                    'unit': '%',
                    'frequency': 'monthly',
                    'base': 3.0, 'variation': 0.15, 'min_val': 2.5, 'max_val': 3.5,
                },
            ],
            'EC': [
                {
                    'name': 'IPC (Inflacion Mensual)',
                    'description': 'Indice de Precios al Consumidor mensual de Ecuador',
                    'unit': '%',
                    'frequency': 'monthly',
                    'base': 2.8, 'variation': 0.3, 'min_val': 1.5, 'max_val': 4.2,
                },
                {
                    'name': 'Precio Petroleo WTI',
                    'description': 'Precio del barril de petroleo West Texas Intermediate',
                    'unit': 'USD',
                    'frequency': 'daily',
                    'base': 80, 'variation': 4, 'min_val': 68, 'max_val': 92,
                },
            ],
            'BR': [
                {
                    'name': 'IPCA (Inflacion)',
                    'description': 'Indice Nacional de Precios al Consumidor Amplio de Brasil',
                    'unit': '%',
                    'frequency': 'monthly',
                    'base': 5.0, 'variation': 0.4, 'min_val': 3.8, 'max_val': 6.5,
                },
                {
                    'name': 'Tipo de Cambio USD/BRL',
                    'description': 'Reales brasileños por dolar estadounidense',
                    'unit': 'BRL',
                    'frequency': 'daily',
                    'base': 5.2, 'variation': 0.2, 'min_val': 4.8, 'max_val': 6.2,
                },
            ],
        }

        today = date.today()

        for country_code, vars_list in variables_config.items():
            country = countries[country_code]

            for var_config in vars_list:
                var, created = MacroVariable.objects.get_or_create(
                    country=country,
                    name=var_config['name'],
                    defaults={
                        'description': var_config['description'],
                        'unit': var_config['unit'],
                        'frequency': var_config['frequency'],
                        'is_active': True,
                    }
                )

                if created:
                    self.stdout.write(f'  Variable creada: {country.code} - {var_config["name"]}')

                if created or not var.historical_data.exists():
                    values = gen_series(
                        var_config['base'],
                        var_config['variation'],
                        months=12,
                        min_val=var_config['min_val'],
                        max_val=var_config['max_val'],
                    )

                    prev_value = None
                    for i, value in enumerate(values):
                        hist_date = today - timedelta(days=(11 - i) * 30)

                        is_anomaly = False
                        if prev_value and prev_value != 0:
                            change_pct = abs((value - prev_value) / prev_value * 100)
                            is_anomaly = change_pct > 8.0

                        HistoricalData.objects.get_or_create(
                            variable=var,
                            date=hist_date,
                            defaults={
                                'value': value,
                                'is_anomaly': is_anomaly,
                            }
                        )
                        prev_value = value

                    self.stdout.write(f'    -> 12 registros historicos generados')

        self.stdout.write(self.style.SUCCESS('\nDatos precargados cargados exitosamente!'))
        self.stdout.write('   Corre el servidor con: python manage.py runserver')
