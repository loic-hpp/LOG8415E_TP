from argparse import ArgumentParser
import re

targets = []
input_file = 'samplegraph.txt'
output_file = 'sampleoutput.txt'\

def distribute(filename, n= 1):
    # checks the amounts of lines in the file and distributes them equally to n mappers
    with open(filename, 'r') as f:
        lines = f.readlines()

    # yield a chunk of lines for each mapper
    chunk_size = len(lines) // n
    for i in range(n):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i != n - 1 else len(lines)
        yield lines[start:end]
    
def map1(chunk):
    # yield user and friends list for each line in the chunk
    for line in chunk:
        if not line.strip():
            continue
        person, *friends = re.split(r'[\t ,]+', line.strip())
        yield int(person), list(map(int, friends))

def map2(user, friends):
    for friend in friends:
        a,b = (user, friend) if user < friend else (friend, user)
        if a in targets or b in targets:
            yield ((a, b), float('-inf'))

    for i in range(len(friends)):
        for j in range(i + 1, len(friends)):
            a,b = (friends[i], friends[j]) if friends[i] < friends[j] else (friends[j], friends[i])
            if a in targets or b in targets:
                yield ((a, b), 1)

def reduce1(key, values):
    common_count = sum(values)  # -inf if edge present
    if common_count > 0:
        a, b = key
        if a in targets:  # emit only to targets
            yield (a, (b, common_count))
        if b in targets:
            yield (b, (a, common_count))
    
def reduce2(user, friend_counts, n=10):
    sorted_friends = sorted(friend_counts, key=lambda x: x[1], reverse=True) 
    top_n = sorted_friends[:n]
    
    # write results to output file <User><TAB><Recommendations>
    with open(output_file, 'a') as f:
        recommendations = ','.join(f"{friend}:{count}" for friend, count in top_n)
        f.write(f"{user}\t{recommendations}\n")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--input', type=str, default='commonfriends/samplegraph.txt', help='Input file path')
    parser.add_argument('--output', type=str, default='commonfriends/sampleoutput.txt', help='Output file path')
    parser.add_argument('--targets', type=str, default=[0,4,5,6], nargs='+', help='List of target user IDs for recommendations')
    args = parser.parse_args()

    input_file = args.input
    output_file = args.output 
    targets = list(map(int, args.targets)) if args.targets else []

    open(output_file, 'w').close()

    # Map phase
    intermediate = {}
    for chunk in distribute(input_file, n=4):
        for user, friends in map1(chunk):
            for key, value in map2(user, friends):
                if key not in intermediate:
                    intermediate[key] = []
                intermediate[key].append(value)

    # # First Reduce phase
    intermediate2 = {}
    for key, values in intermediate.items():
        for k2, v2 in reduce1(key, values):
            if k2 not in intermediate2:
                intermediate2[k2] = []
            intermediate2[k2].append(v2)

    # # Second Reduce phase
    for user in targets:
        friend_counts = intermediate2.get(user, [])
        reduce2(user, friend_counts, n=10)
