from gva.services import GoogleTaskOperator
from flask import Flask, request, jsonify, make_response, render_template
import gva.logging
from gva.data.readers import Reader, FileReader
from gva.data.formats import dictset
import datetime

CREDENTIALS_FILE = "bqro.json"

app = Flask(__name__)

logger = gva.logging.get_logger()
logger.setLevel(10)

def stringify(pager):
    result = []
    page = next(pager, [])
    for item in page:
        result.append(item.decode())
    return ''.join(result)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/search', methods=['GET'])
@app.route('/search/<query>/<start_date>/<end_date>/', methods=['GET'])
def search_query(query=None, start_date=None, end_date=None):

    def search_term(term):
        if term is None or len(term) == 0:
            return None
        return lambda x: term in x['tweet'].lower()

    def date_term(term):
        print(term, datetime.date.fromisoformat(term))
        if term is None or len(term) == 0:
            return None
        return datetime.date.fromisoformat(term)

    r = Reader(
        #fork_processes=True,
        #thread_count=4,
        reader=FileReader,
        from_path='C:/Users/justi/Desktop/month_%m/day_%d/',
 #       data_format='text',
        start_date=date_term(start_date),
        end_date=date_term(end_date),
        #select=['username'],
        where=search_term(query.lower())
)

    pages = dictset.page_dictset(r, 25)
    page = next(pages, [])

    return jsonify(list(page))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=2100, debug=True)
 