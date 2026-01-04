import re

class Parser():
    graph: dict = None
    
    def parse_txt(self, filename):
        graph = {}
        with open(filename, 'r') as file:
            for line in file:
                person, *friends = re.split(r'[\t ,]+', line.strip())
                if friends:
                    graph[int(person)] = list(map(int, friends))
        return graph
    
if __name__ == "__main__":
    parser = Parser()
    graph = parser.parse_txt('commonfriends/samplegraph.txt')
    print(graph)