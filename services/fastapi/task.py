async def simple_task(datetime_, title, text):
    x_count = 0
    m_count = 0
    for message in text.split('\n'):
        m_count += 1
        x_count += message.count('X')
    print(f'datetime_: {datetime_}, title: {title}, x_avg_count_in_line: {x_count / m_count :.3f}')
