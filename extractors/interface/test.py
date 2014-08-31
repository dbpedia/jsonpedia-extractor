import requests

code = """
query = {"query": {"bool": {"must": [{"term": {"section_title": "discography"}}]}}}

def process_data(document, dom):
    return "pippo", "pluto"
"""

def main():
    res = requests.post(
        "http://localhost:5000/es/",
        data={'code': code}
    )

    print res.status_code
    print res.text

if __name__ == '__main__':
    main()