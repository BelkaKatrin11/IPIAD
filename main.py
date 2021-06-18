import json
import string

from rss_parser import Parser
from requests import get
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from datasketch import MinHash

LAST_ARTICLES = 100
INDEX_NAME = 'articles'


def make_item_data(rss_data, article):
    return {'title': rss_data.title,
            'description': rss_data.description,
            'link': rss_data.link,
            'category': rss_data.category,
            'article': article
            }


def download_articles(data_links):
    articles = []
    for link in data_links:
        data = get(link.link)
        if data.status_code == 200:
            html = data.text
            soup = BeautifulSoup(html, 'lxml')
            pages = soup.findAll('p')

            article = []
            for page in pages:
                article.append(page.getText())
            articles.append(make_item_data(link, "".join(article)))
    return articles


def get_non_existent_articles(rss_data):
    search_object = {'size': LAST_ARTICLES, 'query': {'match_all': {}}}
    existent = Elasticsearch().search(index=INDEX_NAME, body=json.dumps(search_object))

    existent_links = []
    for hit in existent['hits']['hits']:
        existent_links.append(hit["_source"]["link"])

    articles = []
    for data in rss_data:
        if data.link not in existent_links:
            print('link', data.link)
            articles.append(data)

    return articles


def create_index():
    es = Elasticsearch()
    created = False
    # index settings
    settings = {
        "settings": {
            "analysis": {
                "filter": {
                    "delimiter": {
                        "type": "word_delimiter",
                        "preserve_original": "true"
                    },
                    "jmorphy2_russian": {
                        "type": "jmorphy2_stemmer",
                        "name": "ru"
                    }
                },
                "analyzer": {
                    "text_ru": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "delimiter",
                            "jmorphy2_russian"
                        ]
                    }
                }
            }
        },
        "mappings": {
                "dynamic": "strict",
                "properties": {
                    "title": {
                        "type": "keyword",
                    },
                    "link": {
                        "type": "keyword"
                    },
                    "description": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        },
                        "analyzer": "text_ru"
                    },
                    "pubdate": {
                        "type": "date"
                    },
                    "category": {
                        "type": "keyword"
                    },
                    "article": {
                        "type": "text",
                        "analyzer": "text_ru"
                    }
                }
            }
    }

    try:
        if not es.indices.exists(INDEX_NAME):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es.indices.create(index=INDEX_NAME, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(record):
    es = Elasticsearch()
    is_stored = True
    try:
        es.index(index=INDEX_NAME, body=record)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def get_rss_data():
    rss_url = "https://lenta.ru/rss/articles"
    xml = get(rss_url)

    if xml.status_code != 200:
        print(xml.status_code)

    parser = Parser(xml=xml.content)
    feed = parser.parse()

    print(feed.language)
    print(feed.version)

    for item in feed.feed:
        print(item.title)
        print(item.description)
        print(item.link)
        print(item.category)

    return feed.feed


def query_by_key(key):
    search_object = {'size': LAST_ARTICLES, 'query': {'query_string': {'query': key}}}
    res = Elasticsearch().search(index=INDEX_NAME, body=json.dumps(search_object))

    found = []
    for hit in res['hits']['hits']:
        found.append(hit["_source"])

    if found is not None:
        print('Found such links by key', key)
        for item in found:
            print(item['link'])
            print(item['title'])
            print(item['category'])


def term_aggregation(field):
    search_object = {'aggs': {field: {'terms': {'field': field}}}}
    res = Elasticsearch().search(index=INDEX_NAME, body=json.dumps(search_object))

    print('\nTERM AGGREGATION RESULT')
    for stat in res['aggregations'][field]['buckets']:
        print(stat['key'], stat['doc_count'])


def cardinality_aggregation(field):
    card = {
        "aggs": {
            field: {
                "cardinality": {
                    "field": field
                }
            }
        }
    }
    res = Elasticsearch().search(index=INDEX_NAME, body=json.dumps(card))
    print('\nCARDINALITY AGGREGATION RESULT')
    print('Approximate count of unique values = ', res['aggregations']['category']['value'])


def shingle(text, k):
    text = text.translate(str.maketrans('', '', string.punctuation))

    text_for_shingle = text.split(' ')

    shingle_set = []

    for i in range(len(text_for_shingle) - k):
        shingle_set.append("".join(text_for_shingle[i:i + k]))

    return shingle_set


def minhash():
    res = Elasticsearch().search(index=INDEX_NAME, body={'size': 2, 'query': {'match_all': {}}})

    first = res['hits']['hits'][0]['_source']['article']
    print('First article', res['hits']['hits'][0]['_source']['title'])
    second = res['hits']['hits'][1]['_source']['article']
    print('Second article', res['hits']['hits'][1]['_source']['title'])

    set1 = shingle(first, 3)
    set2 = shingle(second, 3)

    mh1, mh2 = MinHash(), MinHash()
    for el in set1:
        mh1.update(el.encode('utf8'))
    for el in set2:
        mh2.update(el.encode('utf8'))
    print('\nJaccard', mh1.jaccard(mh2))


def main():
    created = create_index()
    if not created:
        return

    rss_data = get_rss_data()

    articles_links = get_non_existent_articles(rss_data)

    articles = download_articles(articles_links)

    for article in articles:
        store_record(article)

    query_by_key('')

    term_aggregation('category')

    cardinality_aggregation('category')

    minhash()


if __name__ == '__main__':
    main()
