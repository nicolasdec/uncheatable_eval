import time
import requests
from bs4 import BeautifulSoup
import json
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

class EBCCrawler:
    def __init__(self):
        self.stop_event = threading.Event()
        self.base_url = "https://agenciabrasil.ebc.com.br"  # Base URL

    def get_search_results(self, base_url, page=1, retries=5, sleep_time=1, proxy=None):
        url = f"{base_url}?page={page}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        }
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, proxies=proxy, timeout=5)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    search_results = soup.find_all('a', href=True)
                    links = [self.base_url + link['href'] if link['href'].startswith('/') else link['href']
                             for link in search_results if any(sub in link['href'] for sub in ['ultimas', 'direitos-humanos', 'economia', 'educacao', 'geral'])]
                    print(f"Found {len(links)} links on {url}")
                    return links
                else:
                    print(f"Failed to retrieve {url} with status code {response.status_code}")
            except Exception as e:
                print(f"Exception occurred for {url}: {e}")
            time.sleep(sleep_time)
        print("Failed to retrieve data after multiple attempts.")
        return []

    def extract_content_from_url(self, url, retries=3, delay=2):
        for attempt in range(retries):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    html_content = response.content
                    soup = BeautifulSoup(html_content, 'html.parser')
                    article = soup.find('article')
                    if article:
                        texts = [p.get_text() for p in article.find_all('p')]
                        return '\n'.join(texts)
                else:
                    print(f"Failed to load page {url} with status code {response.status_code}")
            except Exception as e:
                print(f"Exception while extracting from {url}: {e}")
            time.sleep(delay)
        return None

    def producer(self, search_urls, url_queue, max_pages=5):
        for url in search_urls:
            for page in range(1, max_pages + 1):
                search_links = self.get_search_results(url, page=page)
                for link in search_links:
                    url_queue.put(link)

    def consumer(self, url_queue, all_news, max_samples):
        while not self.stop_event.is_set():
            url = url_queue.get()
            if url is None:
                break
            content = self.extract_content_from_url(url)
            if content:
                all_news.append(content)
                if len(all_news) >= max_samples:
                    self.stop_event.set()
            url_queue.task_done()

    def pipeline(self, max_samples=1000, max_workers=16, max_pages=5):
        search_urls = [
            'https://agenciabrasil.ebc.com.br/internacional',
            'https://agenciabrasil.ebc.com.br/direitos-humanos',
            'https://agenciabrasil.ebc.com.br/economia',
            'https://agenciabrasil.ebc.com.br/educacao',
            'https://agenciabrasil.ebc.com.br/justica',
            'https://agenciabrasil.ebc.com.br/politica',
            'https://agenciabrasil.ebc.com.br/saude',
            'https://agenciabrasil.ebc.com.br/esportes',   
        ]
        all_news = []
        url_queue = Queue()
        producer_thread = threading.Thread(target=self.producer, args=(search_urls, url_queue, max_pages))
        producer_thread.start()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            consumers = [executor.submit(self.consumer, url_queue, all_news, max_samples) for _ in range(max_workers)]
            producer_thread.join()
            for _ in range(max_workers):
                url_queue.put(None)
            for future in as_completed(consumers):
                future.result()

        return all_news

if __name__ == '__main__':
    crawler = EBCCrawler()
    data = crawler.pipeline(max_samples=500, max_workers=8, max_pages=8)  # Customize the number of pages as needed
    # Save data to JSON with indentation for better readability
    with open("output_data.json", 'w') as f:
        json.dump(data, f, indent=4)
    print("Data has been saved to output_data.json.")