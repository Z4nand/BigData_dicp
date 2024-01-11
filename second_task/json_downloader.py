from mrjob.job import MRJob

from urllib.request import urlopen

from datetime import datetime, timedelta

output_dir = "/home/zanand/Study/PyFolder/bigdata/big_data/second_task/json"

class JsonDownloader(MRJob):
    
    def configure_args(self):
        super(JsonDownloader, self).configure_args()
        self.add_passthru_arg( "--type", default="person", choices=["person", "group", "auditorium"])
        self.add_passthru_arg( "--start_date",  type=str, required=True)
        self.add_passthru_arg( "--end_date",    type=str, required=True)
        
    def mapper(self, _, id):
        start_date = datetime.strptime(self.options.start_date, "%Y.%m.%d")
        end_date   = datetime.strptime(self.options.end_date, "%Y.%m.%d")
        api_type =  self.options.type
        
        date = start_date
        while (date <= end_date):
            
            start = date.strftime("%Y.%m.%d")
            end   = (date + timedelta(days=6)).strftime("%Y.%m.%d")
            
            url = f"https://rasp.omgtu.ru/api/schedule/{api_type}/{id}?start={start}&finish={end}&lng=1"
            
            yield (url, start, end, id) , None
            
            date = date + timedelta(days=7)
            
        
        
    def reducer(self, data, _):
        
        response = urlopen(data[0])
        
        with open(f"{output_dir}/{self.options.type}/{data[3]}_{data[1]}_{data[2]}.json", "wb+") as file:
            file.write(response.read())


if __name__ == '__main__':
    JsonDownloader.run()
