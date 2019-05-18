import csv
output_file = "metricsno.csv"
input_file = "log_no.txt"
schema = ['Time', 'Container Id', 'Total Arrived', 'Total Processed']
with open(output_file, 'wb') as csvfile:
    fw = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    fw.writerow(schema)
    print("Reading from file:" + input_file)
    counter = 0
    arrived = {}
    initialTime = -1
    base = 1
    with open(input_file) as f:
        for line in f:
            split = line.rstrip().split(' ')
            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Arrived:'):
                time = split[2]
                if(initialTime == -1):
                    initialTime = long(time)
                info = "".join(split[5:]).replace(' ','')
                info = info.replace('{','')
                info = info.replace('}','')
                containers = info.split(',')
                total = 0
                for container in containers:
                    if(len(container)>0):
                        Id = container.split('=')[0]
                        value = container.split('=')[1]
                        arrived[Id] = value
                        total += long(value)
                arrived['total'] = total

            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Processed:'):
                time = split[2]
                if(initialTime == -1):
                    initialTime = long(time)
                info = "".join(split[5:]).replace(' ','')
                info = info.replace('{','')
                info = info.replace('}','')
                containers = info.split(',')
                total = 0
                for container in containers:
                    if(len(container)>0):
                        Id = container.split('=')[0]
                        value = container.split('=')[1]
                        total += long(value)
                        if(Id in arrived):
                            fw.writerow([(long(time) - initialTime)/base, Id, arrived[Id], value])
                fw.writerow([(long(time) - initialTime)/base, 'total', arrived['total'], total])

            counter += 1
            if(counter % 100 == 0):
                print("Processed to line:" + str(counter))



