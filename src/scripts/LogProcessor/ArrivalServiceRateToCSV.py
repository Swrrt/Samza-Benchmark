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
containerLongtermDelay = {}
containerLongtermDelayT = {}
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

def parseContainerLongtermDelay(split, fw, base):
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
            if(Id not in containerLongtermDelay):
                containerLongtermDelay[Id] = []
                containerLongtermDelayT[Id] = []
            value = float(value)
            if(value>500): value = 500
            elif(value<0): value = 0
            containerLongtermDelay[Id] += [float(value)]
            containerLongtermDelayT[Id] += [(long(time) - initialTime)/base]
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
            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Longterm' and split[5] == 'Delay:'):
                parseContainerLongtermDelay(split, fw, base)
            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Residual:'):
                parseContainerResidual(split, fw, base)

            #Add migration marker
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
                print(src + ' !!! ' + tgt)

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


            if (split[0] == 'MixedLoadBalanceManager,' and split[5] == 'deployed!' and split[6] <> 'task'):
                src = split[8][:6]
                time = split[2]
                time = (long(time) - initialTime)/base
                if(src not in migrationDeployTime):
                    migrationDeployTime[src] = []
                migrationDeployTime[src] += [time]

            if (split[0] == 'MixedLoadBalanceManager,' and split[5] == 'deployed!' and split[6] == 'task'):
                tgt = split[-1][:6]
                time = split[2]
                time = (long(time) - initialTime)/base
                if(tgt not in migrationDeployTime):
                    migrationDeployTime[tgt] = []
                migrationDeployTime[tgt] += [-time]


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
            lines += [[X, Y, 'y']]
    if(Id not in migrationDeployTime):
        return lines
    for i in range(len(migrationDeployTime[Id])):
        if(migrationDeployTime[Id][i] > 0): #Migrate out
            X = [migrationDeployTime[Id][i]]
            X += [migrationDeployTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'b']]
        else:
            X = [-migrationDeployTime[Id][i]]
            X += [-migrationDeployTime[Id][i]]
            Y = [0]
            Y += [ly]
            lines += [[X, Y, 'r']]

    return lines

import collections
#Arrival Rate and Processing Rate
numberOfContainers = len(containerArrivalRate)
index = 1
fig = plt.figure(figsize=(45, 5 * numberOfContainers))
sortedIds = sorted(containerArrivalRate)
for Id in sortedIds:
    print('Draw arrival rate: {0} {1}'.format(index, Id))
    plt.subplot(numberOfContainers, 1, index)
    index += 1
    legend = ['Arrival Rate', 'Processing Rate']
    plt.plot(containerArrivalRateT[Id], containerArrivalRate[Id],'r^-',containerServiceRateT[Id],containerServiceRate[Id],'bs-')
    lines = addMigrationLine(Id, 1000)
    for line in lines:
        plt.plot(line[0], line[1], linewidth=3.0, color=line[2])
    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (ms)')
    plt.ylabel('Rate (messages per second)')
    plt.title('Container ' + Id + ' Arrival and Service Rate')
    axes = plt.gca()
    axes.set_xlim([50000,450000])
    axes.set_ylim([0,1000])
    #plt.show()
    plt.grid(True)
plt.savefig('figures/ContainerArrivalAndServiceRate.png')
plt.close(fig)

fig = plt.figure(figsize=(45, 5 * numberOfContainers))
index = 1
sortedIds = sorted(containerWindowDelay)
for Id in sortedIds:
    print('Draw window delay: {0} {1}'.format(index, Id))
    plt.subplot(numberOfContainers, 1, index)
    index += 1
    readContainerRealWindowDelay(Id)
    legend = ['Window Delay', 'Real Window Delay', 'Long Term Delay']
    #fig = plt.figure(figsize=(25,15))
    plt.plot(containerWindowDelayT[Id], containerWindowDelay[Id],'bs', containerRealWindowDelayT[Id], containerRealWindowDelay[Id], 'r^', containerLongtermDelayT[Id], containerLongtermDelay[Id], 'cd')
    lines = addMigrationLine(Id, 50)
    for line in lines:
        plt.plot(line[0], line[1], linewidth=3.0, color=line[2])
    plt.legend(legend, loc='upper left')
    plt.xlabel('Time (ms)')
    plt.ylabel('Delay (ms)')
    axes = plt.gca()
    axes.set_xlim([50000,450000])
    axes.set_ylim([0,500])
    plt.title('Container ' + Id + ' Window Delay')
    #plt.show()
    plt.grid(True)
plt.savefig('figures/ContainerWindowDelay.png')
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

fig = plt.figure(figsize=(45, 5 * numberOfContainers))
index = 1
sortedIds = sorted(containerResidual)
for Id in sortedIds:
    print('Draw residual: {0} {1}'.format(index, Id))
    plt.subplot(numberOfContainers, 1, index)
    index += 1
    legend = ['Resdiual']
    #fig = plt.figure(figsize=(25,15))
    plt.plot(containerResidualT[Id], containerResidual[Id],'b^-')
    plt.legend(legend, loc='upper left')
    plt.title('Container ' + Id + ' Residual')
    #plt.show()
    plt.grid(True)
    axes = plt.gca()
    axes.set_xlim([50000,450000])
    axes.set_ylim([0,1000])
plt.savefig('figures/ContainerResidual.png')
plt.close(fig)

