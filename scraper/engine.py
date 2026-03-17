import httpx
import re
from bs4 import BeautifulSoup


# Headers para simular un navegador real y evitar bloqueos
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'es-CO,es;q=0.9,en;q=0.8',
}


def clean_number(text: str) -> float | None:
    """
    Limpia un string de texto y extrae el número flotante.
    Ejemplos:
      "3,45%"   → 3.45
      "$ 4.200" → 4200.0
      "1.234,56"→ 1234.56
    """
    if not text:
        return None

    # Elimina espacios, símbolos comunes y letras
    text = text.strip()
    text = re.sub(r'[^\d.,%\-]', '', text)

    # Maneja formato europeo: 1.234,56 → 1234.56
    if ',' in text and '.' in text:
        if text.index('.') < text.index(','):
            text = text.replace('.', '').replace(',', '.')
        else:
            text = text.replace(',', '')
    elif ',' in text:
        text = text.replace(',', '.')

    # Elimina el símbolo de porcentaje
    text = text.replace('%', '')

    try:
        return float(text)
    except ValueError:
        return None


def scrape_static(url: str, css_selector: str) -> dict:
    """
    Extrae un valor de una página HTML estática usando un selector CSS.
    Retorna un dict con el resultado o el error.
    """
    try:
        response = httpx.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        element = soup.select_one(css_selector)

        if not element:
            return {
                'success': False,
                'error': f'Selector "{css_selector}" no encontró ningún elemento en la página.',
                'raw_text': None,
                'value': None,
            }

        raw_text = element.get_text(strip=True)
        value = clean_number(raw_text)

        if value is None:
            return {
                'success': False,
                'error': f'Se encontró el elemento pero no se pudo extraer un número del texto: "{raw_text}"',
                'raw_text': raw_text,
                'value': None,
            }

        return {
            'success': True,
            'error': None,
            'raw_text': raw_text,
            'value': value,
        }

    except httpx.TimeoutException:
        return {
            'success': False,
            'error': 'La página tardó demasiado en responder (timeout de 15s).',
            'raw_text': None,
            'value': None,
        }
    except httpx.HTTPStatusError as e:
        return {
            'success': False,
            'error': f'Error HTTP {e.response.status_code} al acceder a la URL.',
            'raw_text': None,
            'value': None,
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Error inesperado: {str(e)}',
            'raw_text': None,
            'value': None,
        }


def scrape_dynamic(url: str, css_selector: str) -> dict:
    """
    Extrae un valor de una página dinámica (JavaScript) usando Playwright.
    Se usa cuando scrape_static no funciona porque la página carga datos con JS.
    """
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(extra_http_headers=HEADERS)
            page.goto(url, timeout=30000, wait_until='networkidle')

            # Espera a que el elemento aparezca (máx 10 segundos)
            try:
                page.wait_for_selector(css_selector, timeout=10000)
            except Exception:
                browser.close()
                return {
                    'success': False,
                    'error': f'Selector "{css_selector}" no apareció en la página dinámica.',
                    'raw_text': None,
                    'value': None,
                }

            element = page.query_selector(css_selector)
            raw_text = element.inner_text().strip() if element else None
            browser.close()

        if not raw_text:
            return {
                'success': False,
                'error': 'El elemento fue encontrado pero no tiene texto.',
                'raw_text': None,
                'value': None,
            }

        value = clean_number(raw_text)

        if value is None:
            return {
                'success': False,
                'error': f'Texto encontrado pero sin número extraíble: "{raw_text}"',
                'raw_text': raw_text,
                'value': None,
            }

        return {
            'success': True,
            'error': None,
            'raw_text': raw_text,
            'value': value,
        }

    except Exception as e:
        return {
            'success': False,
            'error': f'Error en Playwright: {str(e)}',
            'raw_text': None,
            'value': None,
        }


def scrape(url: str, css_selector: str, is_dynamic: bool = False) -> dict:
    """
    Función principal del motor.
    Decide automáticamente si usar scraping estático o dinámico.
    """
    if is_dynamic:
        return scrape_dynamic(url, css_selector)
    return scrape_static(url, css_selector)