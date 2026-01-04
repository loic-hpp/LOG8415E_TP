"""
Word Count Algorithm
Counts the frequency of each word in the input text
"""


def map_function(line, emit):
    """
    Map function for word count
    Input: line of text
    Output: (word, 1) for each word
    """
    # Split line into words and emit each word with count 1
    words = line.split()
    for word in words:
        # Clean and normalize the word
        word = word.strip().lower()
        # Remove punctuation
        word = ''.join(c for c in word if c.isalnum())
        if word:
            emit(word, "1")


def reduce_function(key, values):
    """
    Reduce function for word count
    Input: word and list of counts
    Output: word<TAB>total_count
    """
    # Sum all counts for this word
    total = sum(int(v) for v in values)
    return f"{key}\t{total}"


# Algorithm metadata
ALGORITHM_NAME = "Word Count"
ALGORITHM_DESCRIPTION = "Counts the frequency of each word in text files"
INPUT_FORMAT = "Text lines"
OUTPUT_FORMAT = "word<TAB>count"
