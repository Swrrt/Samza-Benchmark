import csv
output_file = "windowno.csv"
input_file = "log_no.txt"

colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
lineType = ['--', '-.', ':']
markerType = ['+', 'o', 'd', '*', 'x']

schema = ['Time', 'Container Id', 'Arrival Rate', 'Service Rate', 'Window Estimate Delay']
containerArrivalRate = {}
containerServiceRate = {}
containerWindowDelay = {}
containerResidual = {}
containerArrivalRateT = {}
containerServiceRateT = {}
containerWindowDelayT = {}
containerResidualT = {}
initialTime = -1
def parseContainerArrivalRate(split, fw, base):
    global initialTime
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
            if(value == 'NaN'): value = '0'
            total += float(value) * 1000
            if(Id not in containerArrivalRate):
                containerArrivalRate[Id] = []
                containerArrivalRateT[Id] = []
            containerArrivalRate[Id] += [float(value)* 1000]
            containerArrivalRateT[Id] += [(long(time) - initialTime)/base]
            fw.writerow([(long(time) - initialTime)/base, Id, value,'', ''])

def parseContainerServiceRate(split, fw, base):
    global initialTime
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
            if(value == 'NaN'): value = '0'
            total += float(value) * 1000
            if(Id not in containerServiceRate):
                containerServiceRate[Id] = []
                containerServiceRateT[Id] = []
            containerServiceRate[Id] += [float(value)* 1000]
            containerServiceRateT[Id] += [(long(time) - initialTime)/base]
            fw.writerow([(long(time) - initialTime)/base, Id, '', value, ''])

def parseContainerWindowDelay(split, fw, base):
    global initialTime
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
            if(Id not in containerWindowDelay):
                containerWindowDelay[Id] = []
                containerWindowDelayT[Id] = []
            containerWindowDelay[Id] += [value]
            containerWindowDelayT[Id] += [(long(time) - initialTime)/base]
            fw.writerow([(long(time) - initialTime)/base, Id, '', '', value])

def parseContainerResidual(split, fw, base):
    global initialTime
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
            total += float(value)
            if(Id not in containerResidual):
                containerResidual[Id] = []
                containerResidualT[Id] = []
            containerResidual[Id] += [value]
            containerResidualT[Id] += [(long(time) - initialTime)/base]
            fw.writerow([(long(time) - initialTime)/base, Id, '', '', value])

with open(output_file, 'wb') as csvfile:
    fw = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    fw.writerow(schema)
    print("Reading from file:" + input_file)
    counter = 0
    arrived = {}

    base = 1
    with open(input_file) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(' ')
            counter += 1
            if(counter % 100 == 0):
                print("Processed to line:" + str(counter))

            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Arrival' and split[5] == 'Rate:'):
                parseContainerArrivalRate(split, fw, base)
            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Service' and split[5] == 'Rate:'):
                parseContainerServiceRate(split, fw, base)
            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Average' and split[5] == 'Delay:'):
                parseContainerWindowDelay(split, fw, base)
            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Residual:'):
                parseContainerResidual(split, fw, base)

import numpy as np
import matplotlib.pyplot as plt

#Arrival Rate and Processing Rate
for Id in containerArrivalRate:
    legend = ['Arrival Rate', 'Processing Rate']
    fig = plt.figure(figsize=(25,15))
    plt.plot(containerArrivalRateT[Id], containerArrivalRate[Id],'r^-',containerServiceRateT[Id],containerServiceRate[Id],'bs-')
    plt.legend(legend, loc='upper left')
    plt.title('Container ' + Id + ' Arrival and Service Rate')
    #plt.show()
    plt.grid(True)
    plt.savefig('figures/Container_'+Id+'_ArrivalAndServiceRate.png')
    plt.close(fig)

for Id in containerWindowDelay:
    legend = ['Window Delay']
    fig = plt.figure(figsize=(25,15))
    plt.plot(containerWindowDelayT[Id], containerWindowDelay[Id],'bs-')
    plt.legend(legend, loc='upper left')
    plt.title('Container ' + Id + ' Window Delay')
    #plt.show()
    plt.grid(True)
    plt.savefig('figures/Container_'+Id+'_WindowDelay.png')
    plt.close(fig)

for Id in containerResidual:
    legend = ['Resdiual']
    fig = plt.figure(figsize=(25,15))
    plt.plot(containerResidualT[Id], containerResidual[Id],'b^-')
    plt.legend(legend, loc='upper left')
    plt.title('Container ' + Id + ' Residual')
    #plt.show()
    plt.grid(True)
    plt.savefig('figures/Container_'+Id+'_Residual.png')
    plt.close(fig)

