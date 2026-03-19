import feedparser
import hashlib
from datetime import datetime
import logging
from nla_engine.vector_store import add_news_to_vector_db

logger = logging.getLogger(__name__)

RSS_FEEDS = [
    # Existentes
    "https://feeds.bbci.co.uk/mundo/rss.xml",
    "https://e00-elmundo.uecdn.es/elmundo/rss/economia.xml",
    # Nuevos — Colombia
    "https://www.portafolio.co/rss/economia.xml",
    "https://www.larepublica.co/rss/finanzas.xml",
    # Nuevos — México
    "https://www.elfinanciero.com.mx/arc/outboundfeeds/rss/economia/",
    # Nuevos — Brasil (UOL)
    "https://rss.uol.com.br/feed/economia.xml",
    # Nuevos — Energía
    "https://www.renewableenergyworld.com/feed/",
    "https://www.pv-magazine-latam.com/feed/",
    # Bloomberg Línea LatAm
    "https://www.bloomberglinea.com/feed/",
]

def fetch_and_vectorize_news():
    """Descarga noticias económicas en RSS y las indexa con Embeddings (ChromaDB)."""
    articles_to_index = []
    
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            # Tomar los 15 artículos más recientes de cada fuente
            for entry in feed.entries[:15]:
                article_id = hashlib.md5(entry.link.encode()).hexdigest()
                title = entry.title
                summary = entry.get('summary', title)
                published = entry.get('published', datetime.now().isoformat())
                
                # Filtrar mínimamente por keywords relevantes si es un feed muy general (opcional)
                text_content = f"{title}. {summary}"
                
                articles_to_index.append({
                    'id': article_id,
                    'text': text_content,
                    'metadata': {
                        'title': title[:100],
                        'url': entry.link,
                        'date': str(published)[:50],
                        'source': url
                    }
                })
        except Exception as e:
            logger.warning(f"Error parseando feed {url}: {e}")
            
    if articles_to_index:
        try:
            add_news_to_vector_db(articles_to_index)
            print(f"Indexados {len(articles_to_index)} artículos en ChromaDB.")
        except Exception as e:
            logger.error(f"Error indexando en VectorStore: {e}")

if __name__ == "__main__":
    fetch_and_vectorize_news()
