import chromadb
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
CHROMA_PATH = BASE_DIR / "chroma_db"

def get_chroma_client():
    return chromadb.PersistentClient(path=str(CHROMA_PATH))

def get_news_collection():
    client = get_chroma_client()
    return client.get_or_create_collection(
        name="economic_news",
        metadata={"hnsw:space": "cosine"}
    )

def add_news_to_vector_db(articles: list[dict]):
    """Indexar noticias en DB vectorial. Articles es lista de dicts con id, text, metadata."""
    if not articles:
        return
        
    collection = get_news_collection()
    
    ids = [a['id'] for a in articles]
    documents = [a['text'] for a in articles]
    metadatas = [a['metadata'] for a in articles]
    
    collection.upsert(
        ids=ids,
        documents=documents,
        metadatas=metadatas
    )

def search_news_context(query: str, n_results: int = 3, filter_metadata: dict = None) -> list:
    collection = get_news_collection()
    
    results = collection.query(
        query_texts=[query],
        n_results=n_results,
        where=filter_metadata
    )
    
    if not results['documents'] or not results['documents'][0]:
        return []
        
    context = []
    for i in range(len(results['documents'][0])):
        context.append({
            'text': results['documents'][0][i],
            'metadata': results['metadatas'][0][i]
        })
    return context
