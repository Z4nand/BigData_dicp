from mrjob.job import MRJob
from mrjob.job import MRStep
from mrjob.protocol import JSONValueProtocol, TextValueProtocol
# this is used to passing whole json file to a mapper
MRJob.INPUT_PROTOCOL = JSONValueProtocol
MRJob.OUTPUT_PROTOCOL = TextValueProtocol


class FindMax(MRJob):

    def mapper(self, _, data):
        for lesson in data:
            yield (lesson["lecturer"], lesson["auditorium"]), 1

    def reducer_count(self, key, values):
        yield key[0], (sum(values), key[1])

    def reducer_max(self, key, values):
        result = max(values, key=lambda t: t[0])
        yield key, f"{key} - {result[1]}: макс кол-во занятий = {result[0]}"

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count),
            MRStep(reducer=self.reducer_max)
        ]


if __name__ == '__main__':
    FindMax.run()