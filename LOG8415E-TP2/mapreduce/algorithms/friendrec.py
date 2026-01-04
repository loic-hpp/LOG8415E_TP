import re

"""
Friend Recommendation Algorithm

This algorithm computes friend recommendations based on common friends.
Input format (per line): <user>\t<friend1> <friend2> ...<friendN>
"""


def map_function(line, emit):
    """
    Mapper for friend recommendations.
    """
    user, *friends = re.split(r'[\t ,]+', line.strip())

    for friend in friends:
        a, b = (user, friend) if user < friend else (friend, user)
        emit((a, b), float('-inf'))

    for i in range(len(friends)):
        for j in range(i + 1, len(friends)):
            a, b = (friends[i], friends[j]) if friends[i] < friends[j] else (friends[j], friends[i])
            emit((a, b), 1)


def reduce_function(key, values):
        """
        Reducer for friend recommendations.
        """
        key = key.strip('()').strip('"').split(',')
        values = list(float(v) for v in values)
        common_count = sum(values)
        if common_count > 0:
                a, b = key[0].strip(), key[1].strip()
                output = [f"{a}\t({b},{common_count})", f"{b}\t({a},{common_count})"]
                return output
        return None

def aggregate_function(reduce_outputs):
    """
    Aggregate reducer outputs into per-user top recommendations.
    """

    user_dict = {}
    for line in reduce_outputs:
        user, rec = line.strip().split('\t')
        friend, count = rec.strip('()').split(',')
        count = int(float(count))
        if user not in user_dict:
            user_dict[user] = []
        user_dict[user].append((friend, count))

    output_lines = []

    for user in user_dict:
        user_dict[user].sort(key=lambda x: x[1], reverse=True)
        user_dict[user] = user_dict[user][:10]
        output_string = f"{user}\t" + ','.join(f"({friend},{count})" for friend, count in user_dict[user])
        output_string = output_string.replace("'", "")
        output_lines.append(output_string)

    return output_lines


# Algorithm metadata
ALGORITHM_NAME = "Friend Recommendation"
ALGORITHM_DESCRIPTION = "Suggests friends based on number of common friends."
INPUT_FORMAT = "user<TAB>friend1 friend2 ..."
OUTPUT_FORMAT = "user<TAB>(friend,count),..."
