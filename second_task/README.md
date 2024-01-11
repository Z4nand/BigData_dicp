## Second task

```bash

# download
python json_downloader.py --start_date 2023.08.28 --end_date 2024.01.01 --type person persons.txt
python json_downloader.py --start_date 2023.08.28 --end_date 2024.01.01 --type group groups.txt
python json_downloader.py --start_date 2023.08.28 --end_date 2024.01.01 --type auditorium auditoriums.txt

# a
python a_subtask/first.py json/person/*.json
python a_subtask/second.py json/person/*.json
python a_subtask/third.py json/person/*.json
python a_subtask/fourth.py json/person/*.json

# b 
python b_subtask/main.py b_subtask/input.json
```
