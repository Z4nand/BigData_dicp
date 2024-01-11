from mrjob.job import MRJob

from mrjob.protocol import JSONValueProtocol, TextValueProtocol
# this is used to passing whole json file to a mapper
MRJob.INPUT_PROTOCOL = JSONValueProtocol
MRJob.OUTPUT_PROTOCOL = TextValueProtocol

class Counter (MRJob):

    def mapper(self, _, data):
        for lesson in data:
            yield lesson["lecturer"], 1 
    
    def reducer(self, key, values):
        yield key, f"{key} - количество занятий = {str(sum(values))}"


if __name__ == '__main__':
    Counter.run()