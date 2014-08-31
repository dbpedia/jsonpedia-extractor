import csv
import json

import click
from jsonpath import jsonpath

from ..base import BaseExtractor


class ParkExtractor(BaseExtractor):
    """
    This extractor will provide a method get_parks
    which will return every tuple (page_title, park_name)
    available
    """
    query = {"query": {"bool": {"must":
                                [{"term": {"section_title": "parks"}}]}}}

    def _get_park_name(self, fragment):
        pieces = []
        if fragment is None:
            return ""

        if isinstance(fragment, basestring):
            pieces.append(fragment.strip().replace("''", ""))
        elif isinstance(fragment, dict):
            if fragment.get('__type', '') == 'reference':
                pieces.append(fragment.get('label'))
            else:
                pieces.append(
                    self._get_park_name(fragment.get('content', None))
                )
        elif isinstance(fragment, list):
            for v in fragment:
                pieces.append(self._get_park_name(v))
        else:
            pieces.append(unicode(fragment))

        return " ".join([p for p in pieces if p is not None]).strip()

    def _get_park_names(self, content):
        if content is None:
            return
        for c in content:
            yield self._get_park_name(c)

    def get_parks(self):
        for document, dom in self.sections_with_content(self.query):
            least_once = False
            if not dom:
                self.processed(False)
                continue

            content = jsonpath(dom, "$..[?(@.__type==\"list_item\")].content")
            if not content:
                self.processed(False)
                continue

            for name in self._get_park_names(content):
                yield document['page_title'], name
                least_once = True

            self.processed(least_once)

@click.command()
@click.option('--elasticsearch', '-e', default='localhost:9200', multiple=True)
@click.option('--format', '-f', default='csv', type=click.Choice(['csv', 'json']))
@click.argument('output', type=click.File('w'))
def main(elasticsearch, format, output):
    hosts = [{'host': h, 'port': int(p)} for h, p in (x.split(':', 1) for x in elasticsearch)]

    pe = ParkExtractor(hosts)

    print "Starting park extractor (output {}): ".format(format)

    if format == 'json':
        out = []
        for p, n in pe.get_parks():
            out.append([p, n])
        json.dump(out, output)
    else:
        writer = csv.writer(output, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["page_title", "album_title"])
        for p, n in pe.get_parks():
            writer.writerow([p.encode('utf-8'), n.encode('utf-8')])

    print "Total: {}".format(pe.report['total'])
    print "Success: {}".format(pe.report['success'])
    print "Failure: {}".format(pe.report['failure'])

if __name__ == '__main__':
    main()
