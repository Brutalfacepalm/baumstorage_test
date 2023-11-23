import requests
from datetime import datetime


url = 'http://0.0.0.0/send_text'

with open('O_Genri_Testovaya_20_vmeste (1).txt', 'r+') as f:
    datetime_ = datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]

    title = "test"
    test_data = f.read()
    message = {"datetime": datetime_, "title": title, "text": test_data}


requests.post(url, json=message)

test_data = test_data.split('\n')
x_count = 0
line_count = 0
for l in test_data:
    l = l.strip()
    if l:
        line_count += 1
        x_count += l.count('Х')


print(x_count, line_count)
