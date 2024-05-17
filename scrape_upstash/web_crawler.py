#m8dhouse - MIT license
import requests
import uuid
from bs4 import BeautifulSoup
from openai import OpenAI
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from urllib.parse import urlparse, urljoin
import mimetypes
from upstash_vector import Index

# Configuration variables
OPENAI_API_KEY = '<YOUR KEY>'
START_URL = '<URL TO CRAWL>'
UPSTASH_TOKEN = '<UPSTASH_TOKEN>'
UPSTASH_API_ENDPOINT = '<UPSTASH_ENDPOINT>'
CHUNK_SIZE = 1000
OVERLAP = 200
MAX_DEPTH = 3
COLLECTION = '<COLLECTION NAME>'

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)
embeddings_model = OpenAIEmbeddings(api_key=OPENAI_API_KEY)

# Initialize the index client
indexupstash = Index(url=UPSTASH_API_ENDPOINT, token=UPSTASH_TOKEN)

# Function to check if a URL points to a non-processable file type
def is_non_processable_url(url):
    non_processable_types = [
        'image', 'video', 'audio', 'application',
        'font', 'model', 'multipart'
    ]
    non_processable_extensions = [
        '.exe', '.dmg', '.zip', '.tar', '.gz', '.rar', '.7z', '.iso',
        '.bin', '.img', '.pdf', '.doc', '.docx', '.ppt', '.pptx',
        '.xls', '.xlsx', '.csv', '.psd', '.ai', '.eps', '.ps', '.ttf',
        '.woff', '.woff2', '.eot', '.otf', '.mp3', '.mp4', '.avi',
        '.mov', '.wmv', '.flv', '.mkv', '.webm', '.wav', '.ogg', '.aac',
        '.m4a', '.bmp', '.gif', '.jpg', '.jpeg', '.png', '.svg', '.tif',
        '.tiff', '.ico', '.raw', '.heic', '.heif'
    ]
    mime_type, _ = mimetypes.guess_type(url)
    if mime_type:
        if any(mime_type.startswith(npt) for npt in non_processable_types):
            return True
    if any(url.lower().endswith(ext) for ext in non_processable_extensions):
        return True
    return False

# Function to get URLs and text content from a webpage
def get_urls_and_text(url, base_domain):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        urls = set()
        text_content = ' '.join(soup.get_text().split())
        title = soup.title.string if soup.title else 'No Title'

        for link in soup.find_all('a', href=True):
            full_url = urljoin(url, link['href'])
            parsed_url = urlparse(full_url)
            if parsed_url.scheme in ['http', 'https'] and parsed_url.netloc.endswith(base_domain):
                if not is_non_processable_url(full_url):
                    urls.add(full_url)

        return urls, text_content, title
    except requests.RequestException as e:
        print(f"Failed to retrieve {url}: {e}")
        return set(), "", "No Title"

# Function to store URLs and embeddings in Upstash
def store_urls_and_embeddings_in_upstash(url, text_content, title):
    # Create the main document
    doc = Document(page_content=text_content, metadata={'url': url, 'title': title})

    # Initialize the text splitter
    splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        model_name="text-embedding-3-small",
        chunk_size=CHUNK_SIZE,
        chunk_overlap=OVERLAP,
    )

    # Split the document into chunks
    chunks = splitter.split_documents([doc])
    print(f"Chunk count: {len(chunks)}\n")

    # Generate a UUID for the document
    document_uuid = uuid.uuid4()

    # Initialize the embedding model
    embedding = OpenAIEmbeddings(api_key=OPENAI_API_KEY)

    for index, chunk in enumerate(chunks):
        text = chunk.page_content
        embedded_text = embedding.embed_query(text)
        chunk_metadata = {
            'url': url,
            'title': title,
            'chunk_index': index,
            'original_text': text_content
        }
        vector_id = f"doc-{document_uuid}-{index}"
        indexupstash.upsert(vectors=[(vector_id, embedded_text, chunk_metadata)])
        print(f"Chunk {index} upserted into Upstash.")



# Function to crawl the website with a depth limit
def crawl_website(start_url, max_depth):
    parsed_start_url = urlparse(start_url)
    base_domain = parsed_start_url.netloc

    visited = set()
    urls_to_visit = [(start_url, 0)]

    while urls_to_visit:
        url, depth = urls_to_visit.pop(0)
        if depth > max_depth:
            continue
        if url not in visited:
            visited.add(url)
            urls, text_content, title = get_urls_and_text(url, base_domain)
            store_urls_and_embeddings_in_upstash(url, text_content, title)
            for new_url in urls:
                if new_url not in visited:
                    urls_to_visit.append((new_url, depth + 1))

# Main script
crawl_website(START_URL, MAX_DEPTH)