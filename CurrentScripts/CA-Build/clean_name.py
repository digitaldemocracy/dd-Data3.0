def clean_name(name, cleaner):
  '''Returns a clean version of a first and last name, based on a callback.

  |name|: The name to clean
  |cleaner|: A function that takes the index of a word in a name and the word.
             Returns the clean version of the word.
  '''
  # Pass each word of the name (and its position in the name) to the
  # name cleaner callback.
  return ' '.join(map(lambda t: cleaner(*t), enumerate(name.split(' '))))
