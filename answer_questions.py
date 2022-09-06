import re

file = open("questions.sql")
sql_script = file.readlines()
# Create an empty command string
sql_command = ''
questions = []
# Iterate over all lines in the sql file
for line in sql_script:
    # Ignore commented lines
    if re.search('--  \d. ', line):
        question = line
        number = re.search('\d', line).group()
    if not line.startswith('--') and line.strip('\n'):
        # Append line to the command string
        sql_command += line.strip('\n')
        # If the command string ends with ';', it is a full statement
        if sql_command.endswith(';'):
            questions.append({'number': number, 'question': question,
                              'sql_command': sql_command})
            sql_command = ''
