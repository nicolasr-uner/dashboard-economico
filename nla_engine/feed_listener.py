import feedparser
import hashlib
from datetime import datetime
from nla_engine.vector_store import add_news_to_vector_db

RSS_FEEDS = [
    "https://feeds.bbci.co.uk/mundo/rss.xml", 
    "https://e00-elmundo.uecdn.es/elmundo/rss/economia.xml",
]

def fetch_and_vectorize_news():
    """Descarga noticias económicas en RSS y las indexa con Embeddings (ChromaDB automatiza al-MiniLM)."""
    articles_to_index = []
    
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:15]:
                article_id = hashlib.md5(entry.link.encode()).hexdigest()
                title = entry.title
                summary = entry.get('summary', title)
                published = entry.get('published', datetime.now().isoformat())
                
                text_content = f"{title}. {summary}"
                
                articles_to_index.append({
                    'id': article_id,
                    'text': text_content,
                    'metadata': {
                        'title': title[:50],
                        'url': entry.link,
                        'date': str(published)[:50],
                        'source': url
                    }
                })
        except Exception as e:
            print(f"Error parseando feed {url}: {e}")
            
    if articles_to_index:
        add_news_to_vector_db(articles_to_index)
        print(f"Indexados {len(articles_to_index)} artículos.")

if __name__ == "__main__":
    fetch_and_vectorize_news()
