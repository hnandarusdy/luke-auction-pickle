import requests

url = "https://www.pickles.com.au/api-website/buyer/ms-web-asset-search/v2/api/product/public/11862/search"

payload = {
    "data": "eyJzZWFyY2giOiIqIiwiZmFjZXRzIjpbInByb2R1Y3RUeXBlL3RpdGxlLHNvcnQ6dmFsdWUiLCJidXlNZXRob2Qsc29ydDp2YWx1ZSIsInNhbHZhZ2Usc29ydDp2YWx1ZSIsIndvdnIsc29ydDp2YWx1ZSJdfQ=="  # short dummy
}

headers = {
    "Content-Type": "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
    "User-Agent": "PostmanRuntime/7.44.1",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Origin": "https://www.pickles.com.au",
    "Referer": "https://www.pickles.com.au/",
    "Cookie": "cf_clearance=YOUR_VALID_CLEARANCE_TOKEN; __cf_bm=YOUR_NEW_COOKIE; _pickles_session=..."
}

response = requests.post(url, headers=headers, data=payload)
print(response.text)

