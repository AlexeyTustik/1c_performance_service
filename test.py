import main

result = main.parse_logfile(r'C:\Users\Pr\Downloads\log1c\rphost_1059522\24082420.log')
csv_data = main.data_to_csv_string(result)
with open(r'C:\Users\Pr\Downloads\log1c\test.csv', 'w+') as f:
    f.write(csv_data)
    f.close()
