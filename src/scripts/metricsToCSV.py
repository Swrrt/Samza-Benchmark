import csv
output_file = "metrics.csv"
input_file = "WordCounter_10_AOL_original.txt"
schema = ['Time', 'Container Id', 'Container Throughput', 'Total Throughput']
with open(output_file, 'wb') as csvfile:
    fw = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    fw.writerow(schema)
    print("Reading from file:" + input_file)
    counter = 0
    with open(input_file) as f:
        for line in f:
            fw.writerow(line.rstrip().split(' '))
            counter += 1
            if(counter % 100 == 0):
                print("Processed to line:" + str(counter))
    
    
            
