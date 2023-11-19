import requests
from datetime import datetime


url = 'http://0.0.0.0/texts'

with open('O_Genri_Testovaya_20_vmeste (1).txt', 'r+') as f:
    datetime_ = datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]

    title = "test"
    test_data = f.read()
    message = [{"datetime_": datetime_, "title": title, "text": test_data}]


requests.post(url, json=message)
