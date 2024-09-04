import requests
import time
import csv


class LitresAPI:
    def __init__(self, api_url='https://api.litres.ru'):
        self.api_url = api_url
        self.headers = {'Content-Type': 'application/json'}
        self.timeout = 1

    def _request_api(self, url):
        try:
            time.sleep(self.timeout)
            response = requests.get(f"{self.api_url }/foundation{url}",
                        headers=self.headers)
            response.raise_for_status()
            data = response.json()
            return data
        except Exception as e:
            raise

    def get_books_by_genre_id(self, genre_id):
        current_url = f'/api/genres/{genre_id}/arts/facets?art_types=text_book'
        while current_url:
            result = self._request_api(current_url)
            books = result['payload']['data']
            for book in books:
                yield book
            current_url = result['payload']['pagination']['next_page']

    def get_book_by_id(self, book_id):
        result = self._request_api(f'/api/arts/{book_id}')
        book = result['payload']['data']
        return book
    
    def get_book_reviews_by_id(self, book_id):
        current_url = f'/api/arts/{book_id}/reviews'
        while current_url:
            result = self._request_api(current_url)
            books = result['payload']['data']
            for book in books:
                yield book
            current_url = result['payload']['pagination']['next_page']

GENRE_ID=5272

def build_data_set():
    api = LitresAPI()

    data = []

    for book in api.get_books_by_genre_id(GENRE_ID):
        book_id = book.get('id', '')

        book_info = api.get_book_by_id(book_id)
        book_additional_info = book_info.get('additional_info', {})
        book_reviews = [review.get('text', '') for review in api.get_book_reviews_by_id(book_id)]

        book_persons = book.get('persons', [])
        book_ratings = book.get('rating', {})
        book_prices = book.get('prices', {})

        data_item = {
            'name': book.get('title', ''),
            'authors': [person.get('full_name', '') for person in book_persons if person.get('role', '') == 'author'],
            'link': book.get('url', ''),
            'rating': book_ratings.get('rated_avg', ''),
            'rating_count': book_ratings.get('rated_total_count', ''),
            'review_count': len(book_reviews),
            'pages_count': book_additional_info.get('current_pages_or_seconds', ''),
            'price': book_prices.get('full_price', ''),
            'text_reviews': book_reviews,
            'age': book.get('min_age', ''),
            'year': book.get('date_written_at', '')
        }
        print(data_item['name'])
        data.append(data_item)


    keys = data[0].keys()

    with open('litres_dataset.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)


if __name__ == '__main__':
    build_data_set()