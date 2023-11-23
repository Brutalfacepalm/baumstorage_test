import requests
from requests.exceptions import ReadTimeout
from datetime import datetime


url = 'http://0.0.0.0:8888/send_text'

with open('O_Genri_Testovaya_20_vmeste (1).txt', 'r+') as f:
    datetime_ = datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]

    title = "test"
    test_data = f.read()
    message = {"datetime": datetime_, "title": title, "text": test_data}

try:
    requests.post(url, json=message, timeout=1)
except ReadTimeout as e:
    print('SEND DONE, NO WAIT RESPONSE...')


test_data = test_data.split('\n')
x_count = 0
line_count = 0
for line in test_data:
    line_count += 1
    x_count += line.count('Х')


print(f'CORRECT RESULT TEST FILE: x_count: {x_count}, line_count: {line_count}, and x_avg_count_in_line: {x_count/line_count:.3f}')
