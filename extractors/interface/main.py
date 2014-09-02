import json
from flask import Flask, request, make_response

import itertools

from ..base import SimpleExtractor
from .settings import ES_SETTINGS

app = Flask(__name__)


def make_json(data, code=200):
    res = make_response(
        json.dumps(data),
        code
    )
    res.headers['Content-Type'] = 'application/json'
    return res

@app.route('/es', methods=['POST'])
def slash():
    code = request.form.get('code', '')
    if not code:
        return make_json(
            {'success': False, 'message': 'code not provided'},
            400
        )

    g, l = {}, {}
    try:
        exec(code, g, l)
    except BaseException as e:
        return make_json(
            # {'success': False, 'message': 'error while executing code'},
            {'success': False, 'message': str(e)},
            400
        )

    query, function = l.get('query'), l.get('process_data')

    if not query or not function:
        return make_json(
            {'success': False, 'message': 'query or process_data missing from code'},
            400
        )

    s = SimpleExtractor(ES_SETTINGS)
    try:
        # limited to 100 elements
        data = list(itertools.islice(s.get_values(query, function), 100))
    except BaseException as e:
        return make_json(
            {'success': False, 'message': str(e)},
            400
        )
        print e.message

    return make_json({'success': True, 'data': data})

if __name__ == '__main__':
    app.run()
