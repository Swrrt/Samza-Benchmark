# -*- coding: utf-8 -*-
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
containerRealWindowDelay = {}
containerResidual = {}
containerArrivalRateT = {}
containerServiceRateT = {}
containerWindowDelayT = {}
containerRealWindowDelayT = {}
containerResidualT = {}
overallWindowDelay = []
overallRealWindowDelay = []
overallWindowDelayT = []
overallRealWindowDelayT = []

migrationDecisionTime = {}
migrationDeployTime = {}

scalingDecisionTime = {}
scalingDeployTime = {}

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
    global initialTime, overallWindowDelayT, overallWindowDelay
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

            if(len(overallWindowDelayT) == 0 or overallWindowDelayT[-1] < (long(time) - initialTime)/base):
                overallWindowDelayT += [(long(time) - initialTime)/base]
                overallWindowDelay += [float(value)]
            elif(overallWindowDelay[-1] < float(value)):
                overallWindowDelay[-1] = float(value)


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

def readContainerRealWindowDelay(Id):
    global initialTime, overallWindowDelayT, overallWindowDelay, overallRealWindowDelayT, overallRealWindowDelay
    fileName = "container" + Id + ".txt"
    counter = 1
    processed = 0
    size = 0
    base = 1
    queue = []
    total = 0;
    lastTime = -100000000
    with open(fileName) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(' ')
            if(split[0] == 'Partition'):
                split = split[1].split(',')
                partition = split[0]
                processed += 1
                time = split[2]
                time = (long(time) - initialTime)/base
                queue += [[time, float(split[1])]]
                total += float(split[1])
                while(queue[0][0] < time - 10000):
                    total -= queue[0][1]
                    queue = queue[1:]


                if(lastTime <= time - 500):
                    if(Id not in containerRealWindowDelay):
                        containerRealWindowDelayT[Id] = []
                        containerRealWindowDelay[Id] = []
                    containerRealWindowDelayT[Id] += [time]
                    containerRealWindowDelay[Id] += [total/len(queue)]


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
            if (split[0] == 'MigratingOnceBalancer:' and split[2] == 'best' and split[-1] <> '[]'):
                src = split[9]
                tgt = split[12][:-1]
                time = lines[i+1].rstrip().split(' ')[2]
                time = (long(time) - initialTime)/base
                if(src not in migrationDecisionTime):
                    migrationDecisionTime[src] = []
                migrationDecisionTime[src] += [time]
                if(tgt not in migrationDecisionTime):
                    migrationDecisionTime[tgt] = []
                migrationDecisionTime[tgt] += [-time]

            if (split[0] == 'MigrateLargestByNumberScaler:' and split[3] == 'scale' and split[4] == 'out'):
                src = split[-1]
                time = lines[i+3].rstrip().split(' ')[2]
                time = (long(time) - initialTime)/base
                tgt = str(len(lines[i+3].rstrip().split(' ')) - 5 + 1).zfill(6)
                if(src not in migrationDecisionTime):
                    migrationDecisionTime[src] = []
                migrationDecisionTime[src] += [time]
                if(tgt not in migrationDecisionTime):
                    migrationDecisionTime[tgt] = []
                migrationDecisionTime[tgt] += [-time]
                print(src + ' !!! ' + tgt)


import numpy as np
import matplotlib.pyplot as plt

def addMigrationLine(Id, ly):
    lines = []
    if(Id not in migrationDecisionTime):
        return lines
    for i in range(len(migrationDecisionTime[Id])):
        if(migrationDecisionTime[Id][i] > 0): #Migrate out
            X = [migrationDecisionTime[Id][i]]
            X += [migrationDecisionTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'g']]
        else:
            X = [-migrationDecisionTime[Id][i]]
            X += [-migrationDecisionTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'r']]

    return lines

#Arrival Rate and Processing Rate
for Id in containerArrivalRate:
    legend = ['Arrival Rate', 'Processing Rate']
    fig = plt.figure(figsize=(25,15))
    plt.plot(containerArrivalRateT[Id], containerArrivalRate[Id],'r^-',containerServiceRateT[Id],containerServiceRate[Id],'bs-')

    lines = addMigrationLine(Id, 1000)
    for line in lines:
        plt.plot(line[0], line[1], linewidth=6.0, color=line[2])

    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (ms)')
    plt.ylabel('Rate (messages per second)')
    plt.title('Container ' + Id + ' Arrival and Service Rate')
    axes = plt.gca()
    axes.set_xlim([0,250000])
    #plt.show()
    plt.grid(True)
    plt.savefig('figures/Container_'+Id+'_ArrivalAndServiceRate.png')
    plt.close(fig)


for Id in containerWindowDelay:
    readContainerRealWindowDelay(Id)
    legend = ['Window Delay', 'Real Window Delay']
    fig = plt.figure(figsize=(25,15))
    plt.plot(containerWindowDelayT[Id], containerWindowDelay[Id],'bs', containerRealWindowDelayT[Id], containerRealWindowDelay[Id], 'r^')

    lines = addMigrationLine(Id, 50)
    for line in lines:
        plt.plot(line[0], line[1], linewidth=6.0, color=line[2])


    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (ms)')
    plt.ylabel('Delay (ms)')
    axes = plt.gca()
    axes.set_xlim([0,250000])
    plt.title('Container ' + Id + ' Window Delay')
    #plt.show()
    plt.grid(True)
    plt.savefig('figures/Container_'+Id+'_WindowDelay.png')
    plt.close(fig)
legend = ['Window Delay', 'Real Window Delay']
fig = plt.figure(figsize=(25,15))
plt.plot(overallWindowDelayT,overallWindowDelay,'bs')#, containerRealWindowDelayT[Id], containerRealWindowDelay[Id], 'r^')
plt.legend(legend, loc='upper left')
plt.xlabel('Time (ms)')
plt.ylabel('Delay (ms)')
#axes = plt.gca()
#axes.set_ylim([0,200])
plt.title('Overall Window Delay')
plt.grid(True)
plt.savefig('figures/OverallWindowDelay.png')
plt.close(fig)

for Id in containerResidual:
    legend = ['Resdiual']
    fig = plt.figure(figsize=(25,15))
    plt.plot(containerResidualT[Id], containerResidual[Id],'b^-')
    plt.legend(legend, loc='upper left')
    plt.title('Container ' + Id + ' Residual')
    #plt.show()
    plt.grid(True)
    axes = plt.gca()
    axes.set_ylim([0,1000])
    plt.savefig('figures/Container_'+Id+'_Residual.png')
    plt.close(fig)

