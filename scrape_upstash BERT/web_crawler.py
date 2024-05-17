import requests
import uuid
from bs4 import BeautifulSoup
from transformers import BertModel, BertTokenizer
import torch
from urllib.parse import urlparse, urljoin
import mimetypes
from upstash_vector import Index


# Configuration variables
OPENAI_API_KEY = '<YOUR KEY>'
START_URL = '<URL TO CRAWL>'
UPSTASH_TOKEN = '<UPSTASH_TOKEN>'
UPSTASH_API_ENDPOINT = '<UPSTASH_ENDPOINT>'
#CHUNK_SIZE = 1000 not used as model is limited and check by max 512 tokens
OVERLAP = 200
MAX_DEPTH = 3
COLLECTION = '<COLLECTION NAME>'

# Load the Dutch BERT model and tokenizer
tokenizer = BertTokenizer.from_pretrained('GroNLP/bert-base-dutch-cased')
model = BertModel.from_pretrained('GroNLP/bert-base-dutch-cased')

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

# Function to split text into chunks within the token limit
def split_text_into_chunks(text, tokenizer, max_tokens=512):
    tokens = tokenizer.tokenize(text)
    chunks = []
    for i in range(0, len(tokens), max_tokens - OVERLAP):
        chunk_tokens = tokens[i:i + max_tokens]
        chunk_text = tokenizer.convert_tokens_to_string(chunk_tokens)
        chunks.append(chunk_text)
    return chunks

# Function to generate embeddings using the Dutch BERT model
def generate_embeddings(text):
    inputs = tokenizer(text, return_tensors='pt', max_length=512, truncation=True, padding='max_length')
    with torch.no_grad():
        outputs = model(**inputs)
    return outputs.last_hidden_state.mean(dim=1).squeeze().tolist()

# Function to store URLs and embeddings in Upstash
def store_urls_and_embeddings_in_upstash(url, text_content, title):
    # Split the document into chunks
    chunks = split_text_into_chunks(text_content, tokenizer)
    print(f"Chunk count: {len(chunks)}\n")

    # Generate a UUID for the document
    document_uuid = uuid.uuid4()

    total_chunks = len(chunks)

    for index, chunk in enumerate(chunks):
        embedded_text = generate_embeddings(chunk)
        
        chunk_metadata = {
            'url': url,
            'title': title,
            'chunk_index': index,
            'total_chunks': total_chunks,
            'original_text': chunk
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