from __future__ import print_function
from collections import defaultdict
import elasticsearch as es
from elasticsearch.helpers import scan
from jsonpath import jsonpath


class ExtractorError(RuntimeError):
    def __init__(self, *args, **kwargs):
        super(RuntimeError, self).__init__(*args, **kwargs)


class BaseExtractor(object):
    def __init__(self, nodes, index_name="jsonpedia",
                 section_type="section", page_type="page"):
        """
        Initialize the extractor with the given elasticsearch nodes.
        :param nodes: a list of dictionaries in the form {'host': '<hostname>', 'port': '<port number>'}
        :param index_name: name of the index
        :param section_type: name of the type that holds the section documents in elasticsearch
        :param page_type: name of the type that holds the page documents in elasticsearch
        """
        self.index_name = index_name
        self.section_type = section_type
        self.page_type = page_type
        self.client = es.Elasticsearch(hosts=nodes)
        self._page_cache = {}
        self.report = defaultdict(int)  # dictionary with default value 0

    def sections_with_content(self, query):
        """
        This method will yield every document matching the query provided
        and the relevant dom section (if available).
        If the relevant dom fragment is not available (for any reason)
        the second element of the tuple will be False
        :param query: the elasticsearch query
        :return: all the tuples (es_document, dom_fragment)
        """
        for s in self.sections_from_query(query):
            yield (s, self._extract_section_data(s))

    def sections_from_query(self, query):
        """
        This method will yield every document matching the query provided

        :param query: the elasticsearch query
        :return:
        """
        self.report = defaultdict(int)
        for r in scan(client=self.client, query=query,
                      doc_type=self.section_type,
                      _source=True):
            self.report['total'] += 1
            yield r['_source']

    def _page_from_title(self, title):
        """
        This query will get the page with the given title.
        It will then cache it.
        :param title: Title of the page to look for
        """
        if not title in self._page_cache:
            query = {"query": {"match": {"title": title}}}
            res = self.client.search(self.index_name, self.page_type,
                                     body=query, size=1, _source=True)
            if not res['hits']['hits']:
                raise ExtractorError(
                    "page with title '{}' not found".format(title)
                )

            self._page_cache[title] = \
                res['hits']['hits'][0]['_source']['content']

        return self._page_cache[title]

    def processed(self, success=True):
        """
        Indicates that a page has been processed (successfully or not)
        This is used in the final report
        """
        if success:
            self.report['success'] += 1
        else:
            self.report['failure'] += 1

    def _extract_section_data(self, section_source):
        """
        Given a section extract the relevant json for it
        :param section_source: the json source of the elasticsearch section docuement
        :return: the json representation of the section's content
        """
        page_title = section_source['page_title']
        section_title = section_source['section_title']
        page_json = self._page_from_title(page_title)
        try:
            return jsonpath(
                page_json,
                "$..wikitext-json.."
                "[?(@.__type==\"section\" and @.title==\"{}\")]".format(section_title)
            )
        except UnicodeEncodeError:
            return False


class SimpleExtractor(BaseExtractor):
    """
    This extractor is meant to be used with simple extraction tasks.
    """

    def get_values(self, query, function):
        """
        This method performs the actual processing of the data.
        It takes a query and a function and applies that function to every
        tuple (section_document, section_dom) returned by the query.
        If the function raises ValueError the processing will be considered
        unsuccessful for that section.
        :param query: the elasticsearch query
        :param function: the function to apply
        """
        for doc, dom in self.sections_with_content(query):
            least_once = False
            try:
                for r in function(doc, dom):
                    yield r
                    least_once = True
            except ValueError:
                pass

            # if at least one result is fetched than this section is
            # considered a success
            self.processed(least_once)
