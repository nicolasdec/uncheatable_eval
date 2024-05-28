import time
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime, timedelta
import argparse
from helpers import save_json
from proxy import ProxyManager
import json
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed


class BBCCrawler:
    def __init__(self):
        self.stop_event = threading.Event()

    @staticmethod
    def generate_dates_y_m_d_compatible(start_date, end_date):
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        step = timedelta(days=1)

        date_list = []
        while start <= end:
            date_str = f"{start.year}-{start.month:02d}-{start.day:02d}"
            date_list.append(date_str)
            start += step

        return date_list

    def get_search_results(self, url, retries=5, sleep_time=1, proxy=None):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        }

        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, proxies=proxy, timeout=5, verify=True)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    search_results = soup.find_all('a')

                    links = [link['href'] for link in search_results if link.has_attr('href')]
                    links = [x for x in links if 'https://www.bbc.com/portuguese/' in x]

                    return links
                else:
                    time.sleep(sleep_time)
            except Exception as e:
                time.sleep(sleep_time)

        print("Failed to retrieve data after multiple attempts.")
        return []

    @staticmethod
    def extract_content_from_url(url, retries=3, delay=2):
        for attempt in range(retries):
            try:
                response = requests.get(url)
                response.raise_for_status()
                html_content = response.content

                soup = BeautifulSoup(html_content, 'html.parser')

                texts = []
                for p in soup.find_all('p'):
                    texts.append(p.get_text())

                return '\n'.join(texts)
            except (requests.RequestException) as e:
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    return None

    def producer(self, dates, search_url, url_queue, proxy_manager):
        for date in dates:
            for url in search_url:
                request_url = url.replace('<DATE>', date)
                proxy = proxy_manager.get_random_proxy()
                search_links = self.get_search_results(request_url, proxy=proxy)
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

    def pipeline(self, start_date, end_date, max_samples=1000, max_workers=16, proxy_manager=None):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").strftime('%Y-%m-%d')
        end_date = datetime.strptime(end_date, "%Y-%m-%d").strftime('%Y-%m-%d')
        search_url = [
            'https://www.bbc.com/portuguese/topics/cvjp2jr0k9rt',
            'https://www.bbc.com/portuguese/topics/cz74k717pw5t'
        ]
        all_news = []
        dates = self.generate_dates_y_m_d_compatible(start_date, end_date)
        url_queue = Queue()
        producer_thread = threading.Thread(target=self.producer, args=(dates, search_url, url_queue, proxy_manager))
        producer_thread.start()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            consumers = [executor.submit(self.consumer, url_queue, all_news, max_samples) for _ in range(max_workers)]
            producer_thread.join()
            for _ in range(max_workers):
                url_queue.put(None)
            for future in as_completed(consumers):
                future.result()

        return all_news[:max_samples]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--start_date', type=str, required=True,
                        help='The start date (YYYY-MM-DD).')
    parser.add_argument('--end_date', type=str, required=True,
                        help='The end date (YYYY-MM-DD).')
    parser.add_argument('--file_name', type=str, required=True,
                        help='JSON file name')

    parser.add_argument('--max_samples', type=int, default=1000,
                        help='Number of news to crawl. Default is 1000.')
    parser.add_argument('--max_workers', type=int, default=16,
                        help='Max worker')

    args = parser.parse_args()

    crawler = BBCCrawler()
    my_manager = ProxyManager()

    data = crawler.pipeline(start_date=args.start_date,
                            end_date=args.end_date,
                            max_samples=args.max_samples,
                            max_workers=args.max_workers,
                            proxy_manager=my_manager)

    save_json(data, args.file_name)
