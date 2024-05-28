import requests
from bs4 import BeautifulSoup
import json
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class SciELOCrawler:
    def __init__(self):
        self.stop_event = threading.Event()
        self.base_url = "https://www.scielo.br"
        self.search_base_url = "https://search.scielo.org"

    def get_search_results(self, base_url, page=1, retries=5, sleep_time=1, proxy=None):
        url = f"{base_url}/?q=*&lang=pt&filter%5Bin%5D%5B%5D=scl&page={page}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        }
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, proxies=proxy, timeout=5)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    search_results = soup.find_all('div', class_='line')
                    links = []
                    for result in search_results:
                        a_tag = result.find('a', href=True)
                        if a_tag:
                            href = a_tag['href']
                            if "scielo.php?script=sci_arttext" in href and "scielo.br" in href:
                                links.append(href)
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
                    article_section = soup.find('div', class_='articleSection')
                    if article_section:
                        abstract_paragraph = article_section.find('p')
                        if abstract_paragraph:
                            return abstract_paragraph.get_text(strip=True)
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

    def consumer(self, url_queue, all_articles, max_samples):
        while not self.stop_event.is_set():
            url = url_queue.get()
            if url is None:
                break
            content = self.extract_content_from_url(url)
            if content:
                all_articles.append(content)
                if len(all_articles) >= max_samples:
                    self.stop_event.set()
            url_queue.task_done()

    def pipeline(self, max_samples=1000, max_workers=16, max_pages=5):
        search_urls = [self.search_base_url]
        all_articles = []
        url_queue = Queue()
        producer_thread = threading.Thread(target=self.producer, args=(search_urls, url_queue, max_pages))
        producer_thread.start()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            consumers = [executor.submit(self.consumer, url_queue, all_articles, max_samples) for _ in range(max_workers)]
            producer_thread.join()
            for _ in range(max_workers):
                url_queue.put(None)
            for future in as_completed(consumers):
                future.result()

        return all_articles

if __name__ == '__main__':
    crawler = SciELOCrawler()
    data = crawler.pipeline(max_samples=500, max_workers=8, max_pages=8)  # Customize the number of pages as needed
    with open("scielo_data.json", 'w') as f:
        json.dump(data, f, indent=4)
    print("Data has been saved to scielo_data.json.")
