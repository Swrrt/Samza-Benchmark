import csv
output_file = "metricsno.csv"
input_file = "log_no.txt"
schema = ['Time', 'Container Id', 'Avg Backlog', 'Arrival Rate', 'Process Rate']

containerAverageBacklog = {}
containerArrivalRate = {}
containerProcessRate = {}
containerAverageBacklogT = {}
containerArrivalRateT = {}
containerProcessRateT = {}

def addAverageBacklog(Id, value, time):
    if(Id not in containerArrivalRate):
        containerAverageBacklog[Id] = []
        containerAverageBacklogT[Id] = []
    containerAverageBacklog[Id] += [value]
    containerAverageBacklogT[Id] += [time]

def addArrivalRate(Id, value, time):
    if(Id not in containerArrivalRate):
        containerArrivalRate[Id] = []
        containerArrivalRateT[Id] = []
    containerArrivalRate[Id] += [value]
    containerArrivalRateT[Id] += [time]

def addProcessRate(Id, value, time):
    if(Id not in containerProcessRate):
        containerProcessRate[Id] = []
        containerProcessRateT[Id] = []
    containerProcessRate[Id] += [value]
    containerProcessRateT[Id] += [time]

with open(output_file, 'wb') as csvfile:
    fw = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    fw.writerow(schema)
    print("Reading from file:" + input_file)
    counter = 0
    initialTime = -1
    base = 1
    with open(input_file) as f:
        for line in f:
            split = line.rstrip().split(' ')
            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Average' and split[5] == 'Backlog:'):
                time = split[2]
                if(initialTime == -1):
                    initialTime = long(time)
                info = "".join(split[6:]).replace(' ','')
                info = info.replace('{','')
                info = info.replace('}','')
                containers = info.split(',')
                total = 0
                for container in containers:
                    if(len(container)>0):
                        Id = container.split('=')[0]
                        value = container.split('=')[1]
                        total += float(value)
                        addAverageBacklog(Id, value, (long(time) - initialTime)/base)
                        fw.writerow([(long(time) - initialTime)/base, Id, value, '', ''])
                addAverageBacklog('total', total, (long(time) - initialTime)/base)
                fw.writerow([(long(time) - initialTime)/base, 'total', total, '', ''])

            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Arrival' and split[5] == 'Rate:'):
                time = split[2]
                if(initialTime == -1):
                    initialTime = long(time)
                info = "".join(split[6:]).replace(' ','')
                info = info.replace('{','')
                info = info.replace('}','')
                containers = info.split(',')
                total = 0
                for container in containers:
                    if(len(container)>0):
                        Id = container.split('=')[0]
                        value = container.split('=')[1]
                        total += float(value)
                        addArrivalRate(Id, value, (long(time) - initialTime)/base)
                        fw.writerow([(long(time) - initialTime)/base, Id, '',  value, ''])
                addArrivalRate('total', total, (long(time) - initialTime)/base)
                fw.writerow([(long(time) - initialTime)/base, 'total', '', total, ''])

            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Processing' and split[5] == 'Speed:'):
                time = split[2]
                if(initialTime == -1):
                    initialTime = long(time)
                info = "".join(split[6:]).replace(' ','')
                info = info.replace('{','')
                info = info.replace('}','')
                containers = info.split(',')
                total = 0
                for container in containers:
                    if(len(container)>0):
                        Id = container.split('=')[0]
                        value = container.split('=')[1]
                        total += float(value)
                        addProcessRate(Id, value, (long(time) - initialTime)/base)
                        fw.writerow([(long(time) - initialTime)/base, Id, '', '', value])
                addProcessRate('total', total, (long(time) - initialTime)/base)
                fw.writerow([(long(time) - initialTime)/base, 'total', '', '', total])

            counter += 1
            if(counter % 100 == 0):
                print("Processed to line:" + str(counter))

import matplotlib.pyplot as plt
#for Id in containerArrived:
#    plt.plot(containerArrived[Id],containerArrivedT[Id])
#    plot.show()
for Id in containerAverageBacklog:
    plt.plot(containerAverageBacklogT[Id],containerAverageBacklog[Id])
    plt.title('Container ' + Id + ' average backlog')
    plt.show()
for Id in containerArrivalRate:
    plt.plot(containerArrivalRateT[Id],containerArrivalRate[Id])
    plt.title('Container ' + Id + ' arrival rate')
    plt.show()
for Id in containerProcessRate:
    plt.plot(containerProcessRateT[Id],containerProcessRate[Id])
    plt.title('Container ' + Id + ' process rate')
    plt.show()

