from django import forms
from .models import Country, MacroVariable


class CountryForm(forms.ModelForm):
    """Formulario para añadir un nuevo país"""
    class Meta:
        model = Country
        fields = ['name', 'code', 'flag_emoji']
        widgets = {
            'name': forms.TextInput(attrs={
                'class': 'w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500',
                'placeholder': 'Ej: Colombia'
            }),
            'code': forms.TextInput(attrs={
                'class': 'w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500',
                'placeholder': 'Ej: CO'
            }),
            'flag_emoji': forms.TextInput(attrs={
                'class': 'w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500',
                'placeholder': 'Ej: 🇨🇴'
            }),
        }
        labels = {
            'name': 'Nombre del país',
            'code': 'Código ISO (2 letras)',
            'flag_emoji': 'Emoji de bandera',
        }


class MacroVariableForm(forms.ModelForm):
    """Formulario para añadir una nueva variable macroeconómica"""
    class Meta:
        model = MacroVariable
        fields = ['country', 'name', 'description', 'source_url', 'css_selector', 'frequency', 'unit', 'is_dynamic']
        widgets = {
            'country': forms.Select(attrs={
                'class': 'w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500',
            }),
            'name': forms.TextInput(attrs={
                'class': 'w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500',
                'placeholder': 'Ej: IPC Colombia'
            }),
            'description': forms.Textarea(attrs={
                'class': 'w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500',
                'rows': 2,
                'placeholder': 'Descripción breve de la variable'
            }),
            'source_url': forms.URLInput(attrs={
                'class': 'w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500',
                'placeholder': 'https://...'
            }),
            'css_selector': forms.TextInput(attrs={
                'class': 'w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500',
                'placeholder': 'Ej: span.valor-ipc'
            }),
            'frequency': forms.Select(attrs={
                'class': 'w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500',
            }),
            'unit': forms.TextInput(attrs={
                'class': 'w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:outline-none focus:border-blue-500',
                'placeholder': 'Ej: %, COP, USD'
            }),
            'is_dynamic': forms.CheckboxInput(attrs={
                'class': 'w-4 h-4 text-blue-500 bg-gray-800 border-gray-700 rounded',
            }),
        }
        labels = {
            'country': 'País',
            'name': 'Nombre de la variable',
            'description': 'Descripción',
            'source_url': 'URL fuente',
            'css_selector': 'Selector CSS',
            'frequency': 'Frecuencia',
            'unit': 'Unidad',
            'is_dynamic': '¿Página dinámica? (usa JavaScript)',
        }