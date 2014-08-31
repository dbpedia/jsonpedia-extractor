import csv
import json
import click
from jsonpath import jsonpath
from ..base import BaseExtractor

class DiscograpyExtractor(BaseExtractor):
    """
    This extractor will provide a method get_discography
    which will return every tuple (page_title, album_title)
    available
    """
    query = {"query": {"bool": {"must":
                                [{"term": {"section_title": "discography"}}]}}}

    def _get_album_name(self, fragment):
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
                    self._get_album_name(fragment.get('content', None))
                )
        elif isinstance(fragment, list):
            for v in fragment:
                pieces.append(self._get_album_name(v))
        else:
            pieces.append(unicode(fragment))

        return " ".join([p for p in pieces if p is not None]).strip()

    def _get_album_names(self, content):
        if content is None:
            return
        for c in content:
            yield self._get_album_name(c)

    def get_discography(self):
        for document, dom in self.sections_with_content(self.query):
            least_once = False
            if not dom:
                self.processed(False)
                continue

            content = jsonpath(dom, "$..[?(@.__type==\"list_item\")].content")
            if not content:
                self.processed(False)
                continue

            for name in self._get_album_names(content):
                yield document['page_title'], name
                least_once = True

            self.processed(least_once)


@click.command()
@click.option('--elasticsearch', '-e', default='localhost:9200', multiple=True)
@click.option('--format', '-f', default='csv', type=click.Choice(['csv', 'json']))
@click.argument('output', type=click.File('w'))
def main(elasticsearch, format, output):
    hosts = [{'host': h, 'port': int(p)} for h, p in (x.split(':', 1) for x in elasticsearch)]

    de = DiscograpyExtractor(hosts)

    if format=='json':
        out = []
        for p, n in de.get_discography():
            out.append([p, n])
        json.dump(out, output)
    else:
        writer = csv.writer(output, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["page_title", "album_title"])
        for p, n in de.get_discography():
            writer.writerow([p.encode('utf-8'), n.encode('utf-8')])

    print "Total: {}".format(de.report['total'])
    print "Success: {}".format(de.report['success'])
    print "Failure: {}".format(de.report['failure'])

if __name__ == '__main__':
    main()
