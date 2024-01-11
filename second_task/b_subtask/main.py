from mrjob.job import MRJob
from mrjob.job import MRStep
from mrjob.protocol import JSONValueProtocol, TextValueProtocol
from urllib.request import urlopen
import json
from datetime import datetime, timedelta
import itertools

# this is used to passing whole json file to a mapper
MRJob.INPUT_PROTOCOL = JSONValueProtocol
MRJob.OUTPUT_PROTOCOL = TextValueProtocol

START_DATE = "2023.12.03"
END_DATE = "2023.12.30"
DATE_FORMAT = "%Y.%m.%d"

LECTURER_MATRIX = list()
AUDITORIUM_MATRIXES = dict()

AUDITORIUMS = {
        178: "8-201",
        176: "8-204",
}
    

def index_to_date(index):
    if index == 0:
        return "08:00"
    elif index == 1:
        return "09:40"
    elif index == 2:
        return "11:35"
    elif index == 3:
        return "13:15"
    elif index == 4:
        return "15:10"
    elif index == 5:
        return "16:50"
    elif index == 6:
        return "18:30"
    
def date_to_index(date):
    if date == "08:00":
        return 0
    elif date == "09:40":
        return 1
    elif date == "11:35":
        return 2
    elif date == "13:15":
        return 3
    elif date == "15:10":
        return 4
    elif date == "16:50":
        return 5
    elif date == "18:30":
        return 6

def matrix_from_lessons(lessons, start_date, end_date):
    start_date = datetime.strptime(start_date, DATE_FORMAT)
    end_date = datetime.strptime(end_date, DATE_FORMAT)

    days = (end_date - start_date).days + 1
    matrix =  [[True] * 7 for _ in range(0, days)]

    for lesson in lessons:
        time_index = date_to_index(lesson["beginLesson"])
        if time_index is None:
            continue

        date = datetime.strptime(lesson["date"], DATE_FORMAT)
        day_index = (date - start_date).days


        matrix[day_index][time_index] = False


    return matrix

def lessons_from_url(url):
 
    resp = urlopen(url)
    data = resp.read()
    return json.loads(data.decode("utf-8"))

def get_lecturer_matrix(id, start_date = START_DATE, end_date = END_DATE):
    url = f"https://rasp.omgtu.ru/api/schedule/person/{id}?start={start_date}&finish={end_date}&lng=1"
 
    lessons = lessons_from_url(url)
    matrix = matrix_from_lessons(lessons, start_date, end_date)
    return matrix

def get_auditorium_matrix(id, start_date = START_DATE, end_date = END_DATE):
    url = f"https://rasp.omgtu.ru/api/schedule/auditorium/{id}?start={start_date}&finish={end_date}&lng=1"

    lessons = lessons_from_url(url)
    matrix = matrix_from_lessons(lessons, start_date, end_date)
    return matrix

def get_group_matrix(id, start_date = START_DATE, end_date = END_DATE):
    url = f"https://rasp.omgtu.ru/api/schedule/group/{id}?start={start_date}&finish={end_date}&lng=1"
 
    lessons = lessons_from_url(url)
    matrix = matrix_from_lessons(lessons, start_date, end_date)
    return matrix

class Runner(MRJob):

    def lessons_mapper(self, _, lessons):
        for lesson in lessons:
            yield None, lesson

    def group_mapper(self, _, lesson):

            yield (lesson["groupOid"], lesson["group"]), lesson

    def group_reducer(self, group, lessons):
        group_id = group[0]
        group_name = group[1]
        group_matrix = get_group_matrix(group_id)
        lecturer_matrix = LECTURER_MATRIX

        start_date = datetime.strptime(START_DATE, DATE_FORMAT)
        end_date = datetime.strptime(END_DATE, DATE_FORMAT)
        days = (end_date - start_date).days + 1

        for lesson in lessons:
            for (day, time) in itertools.product(range(0, days), range(0, 7)):
                
                for (audit_id, audit_matrix) in AUDITORIUM_MATRIXES.items():
                
                    if group_matrix[day][time] and lecturer_matrix[day][time] and audit_matrix[day][time]:
                        
                        # количество пар должно быть меньше 4
                        if(sum(group_matrix[day]) > 4):
                            continue
                        
                        # True - есть пара
                        # False - нет пары
                        prev_time = True if time <= 0 else group_matrix[day][time - 1]
                        next_time = True if time >= 6 else group_matrix[day][time + 1]
                        
                        # у пар не должно быть окон, значит после нашей и до нашей пары должна быть хоть одна пара
                        if prev_time and next_time:
                             continue
                        
                        lecturer_matrix[day][time] = False
                        audit_matrix[day][time] = False
                        group_matrix[day][time] = False
                        
                        yield None, f"[{group_name}] date:{lesson['date']}-{lesson['beginLesson']} => [{AUDITORIUMS[audit_id]}] {(start_date + timedelta(days=day)).date()}:{index_to_date(time)}"
                        break
                else:
                    continue
                break
            
            else:
                # if nothing found
                yield None, f"group:{group_id}, lesson: {lesson['date']}-{lesson['beginLesson']} => NO OPTIONS FOUND :("
                continue
            

    def steps(self):
        return [
            MRStep(mapper=self.lessons_mapper,),
            MRStep(
                mapper=self.group_mapper,
                reducer=self.group_reducer
            )
        ]


if __name__ == '__main__':
    LECTURER_MATRIX = get_lecturer_matrix(782898)
    AUDITORIUM_MATRIXES[178] = get_auditorium_matrix(178) # 8-201
    AUDITORIUM_MATRIXES[176] = get_auditorium_matrix(176) # 8-204

    Runner.run()