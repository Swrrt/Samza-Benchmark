import csv
output_file = "metricsno.csv"
input_file = "log_no.txt"

colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
lineType = ['--', '-.', ':']
markerType = ['+', 'o', 'd', '*', 'x']

schema = ['Time', 'Container Id', 'Total Arrived', 'Total Processed', 'Estimate Delay', 'Total Flush Processed']

containerArrived = {}
containerProcessed = {}
containerFlushProcessed = {}
containerArrivedT = {}
containerProcessedT = {}
containerFlushProcessedT = {}
containerDelay = {}
containerDelayT = {}
containerRealDelay = {}
containerRealDelayT = {}
initialTime = -1
partitionBacklog = {}
partitionBacklogT = {}
partitionArrived = {}
partitionProcessed = {}
partitionArrivedT = {}
partitionProcessedT = {}

def parsePartitionArrived(split, fw, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[5:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    partitions = info.split(',')
    total = 0
    for partition in partitions:
        if(len(partition)>0):
            Id = partition.split('=')[0]
            value = partition.split('=')[1]
            arrived[Id] = value
            total += long(value)
            if(Id not in partitionArrived):
                partitionArrived[Id] = []
                partitionArrivedT[Id] = []
            partitionArrived[Id] += [value]
            partitionArrivedT[Id] += [(long(time) - initialTime)/base]

def parsePartitionProcessed(split, fw, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[5:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    partitions = info.split(',')
    total = 0
    for partition in partitions:
        if(len(partition)>0):
            Id = partition.split('=')[0]
            value = partition.split('=')[1]
            arrived[Id] = value
            total += long(value)
            if(Id not in partitionProcessed):
                partitionProcessed[Id] = []
                partitionProcessedT[Id] = []
            partitionProcessed[Id] += [value]
            partitionProcessedT[Id] += [(long(time) - initialTime)/base]


def parseContainerArrived(split, fw, base):
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
            arrived[Id] = value
            total += long(value)
            if(Id not in containerArrived):
                containerArrived[Id] = []
                containerArrivedT[Id] = []
            containerArrived[Id] += [value]
            containerArrivedT[Id] += [(long(time) - initialTime)/base]
            fw.writerow([(long(time) - initialTime)/base, Id, value,'', '', ''])

def parseContainerProcessed(split, fw, base, processed):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[5:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    containers = info.split(',')
    total = 0
    processed = {}
    for container in containers:
        if(len(container)>0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            total += long(value)
            processed[Id] = value
            if(Id not in containerProcessed):
                containerProcessed[Id] = []
                containerProcessedT[Id] = []
            containerProcessed[Id] += [value]
            containerProcessedT[Id] += [(long(time) - initialTime)/base]
            fw.writerow([(long(time) - initialTime)/base, Id, '', value, '', ''])

def parseContainerDelay(split, fw, base):
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
            if(value=='NaN'):
                value = '0'
            total += float(value)
            if(Id not in containerDelay):
                containerDelay[Id] = []
                containerDelayT[Id] = []
            #containerDelay[Id] += [containerProcessed[Id][-1]] #Check if delay is OK
            #containerDelayT[Id] += [(long(time) - initialTime)/base - float(value)]
            containerDelay[Id] += [value]
            containerDelayT[Id] += [containerProcessed[Id][-1]]
            fw.writerow([(long(time) - initialTime)/base, Id, '','', value, ''])

def parseContainerFlushProcessed(split, fw, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = long(time)
    info = "".join(split[6:]).replace(' ','')
    info = info.replace('{','')
    info = info.replace('}','')
    containers = info.split(',')
    #total = 0
    #processed = {}
    for container in containers:
        if(len(container)>0):
            Id = container.split('=')[0]
            value = container.split('=')[1]
            #total += long(value)
            #processed[Id] = value
            if(Id not in containerFlushProcessed):
                containerFlushProcessed[Id] = []
                containerFlushProcessedT[Id] = []
            containerFlushProcessed[Id] += [value]
            containerFlushProcessedT[Id] += [(long(time) - initialTime)/base]
            fw.writerow([(long(time) - initialTime)/base, Id, '', '','', value])

import matplotlib.pyplot as plt

def parsePartitions(lines, i, Id, base):
    global initialTime
    if(initialTime == -1):
        return 0
    j = i+1
    suffix = lines[i].rstrip().split(' ')[-1]
    partitionBacklog = {}
    partitionBacklogT = []
    cCompleted = []
    cArrived = []
    cT = []
    time = "0"
    while(True):
        line = lines[j]
        split = split = line.rstrip().split(' ')
        if(split[0] == 'DelayEstimator:' and split[2] == 'end'):
            break
        time = split[3]
        arrived = split[5]
        completed = split[7]
        cArrived += [arrived]
        cCompleted += [completed]
        cT += [(long(time) - initialTime)/base]
        info = "".join(split[9:]).replace(' ','')
        info = info.replace('{','')
        info = info.replace('}','')
        partitions = info.split(',')
        partitionBacklogT += [(long(time) - initialTime)/base]
        for partition in partitions:
            if(len(partition)>0):
                partitionId = partition.split('=')[0]
                value = partition.split('=')[1]
                if(partitionId not in partitionBacklog):
                    partitionBacklog[partitionId] = []
                partitionBacklog[partitionId] += [value]
        j+=1
    legend = ['Arrived', 'Processed']

    containerArrived[Id] = cArrived[:]
    containerArrivedT[Id] = cT[:]
    fig = plt.figure(figsize=(25,15))

    plt.plot(cT, cArrived,'r^-', cT, cCompleted,'bs-')
    #print(partitionBacklog)
    #print(partitionBacklogT)

    for i in range(0, len(partitionBacklog)):
        #partitionBacklog
        #if(i == 0):
        #    partitionBacklog[str(i)] = [long(partitionBacklog[str(i)][x]) + long(containerProcessed[Id][x]) for x in range(len(partitionBacklog[str(i)]))]
        #else:
        #    partitionBacklog[str(i)] = [long(partitionBacklog[str(i)][x]) + long(partitionBacklog[str(i-1)][x]) for x in range(len(partitionBacklog[str(i)]))]

        plt.plot(partitionBacklogT, partitionBacklog[str(i)], colors[i%len(colors)] + lineType[i%len(lineType)] + markerType[i%len(markerType)])
        legend += ['Partition ' + str(i)]

    plt.legend(legend, loc='upper left')
    plt.title('Container ' + Id + ' Backlogs at time ' + time + ' ' + suffix)
    axes = plt.gca()
    plt.grid(True)
    axes.set_ylim([0,6000])
    axes.set_xlim([0,160000])
    #plt.show()
    plt.savefig('figures/Container_'+Id+'_BacklogsAtTime_'+time+'_'+suffix+'.png')
    plt.close(fig)
    return j

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
            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Arrived:'):
                parseContainerArrived(split, fw, base)
                #arrived['total'] = total
                #Id = 'total'
                #if(Id not in containerArrived):
                #    containerArrived[Id] = []
                #    containerArrivedT[Id] = []
                #containerArrived[Id] += [value]
                #containerArrivedT[Id] += [(long(time) - initialTime)/base]
                #fw.writerow([(long(time) - initialTime)/base, 'total', total, ''])


                #arrived['total'] = total
                #Id = 'total'
                #if(Id not in containerArrived):
                #    containerArrived[Id] = []
                #    containerArrivedT[Id] = []
                #containerArrived[Id] += [value]
                #containerArrivedT[Id] += [(long(time) - initialTime)/base]
                #fw.writerow([(long(time) - initialTime)/base, 'total', total, ''])

            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Processed:'):
                processed = {}
                parseContainerProcessed(split, fw, base, processed)
                #Id = 'total'
                #if(Id not in containerArrived):
                #    containerProcessed[Id] = []
                #    containerProcessedT[Id] = []
                #containerProcessed[Id] += [value]
                #containerProcessedT[Id] += [(long(time) - initialTime)/base]
                #fw.writerow([(long(time) - initialTime)/base, 'total', '', total])

            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Delay:'):
                parseContainerDelay(split,fw, base)

            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Flush' and split[5] == "Processed:"):
                parseContainerFlushProcessed(split, fw, base)

            counter += 1
            if(counter % 100 == 0):
                print("Processed to line:" + str(counter))

            if (split[0] == 'DelayEstimator:' and split[1] == 'DelayEstimator,' and split[2] == 'show' and split[3] == 'executor'):
                #    if(i>50000):
                #i = parsePartitions(lines, i, split[4], base)
                1+2

            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Partition' and split[5] == "Processed:"):
                split = line.replace('Partition ', '').split(' ')
                parsePartitionProcessed(split,fw,base)

            if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Partition' and split[5] == "Arrived:"):
                split = line.replace('Partition ', '').split(' ')
                parsePartitionArrived(split,fw,base)

partitionRealDelay = {}
partitionRealDelayT = {}
containerPartition = {}
def readContainerRealDelay(Id):
    fileName = "container" + Id + ".txt"
    counter = 1
    processed = 0
    size = 0
    base = 1
    containerRealDelay[Id] = ['0']
    containerRealDelayT[Id] = ['0']
    containerPartition[Id] = []
    queue = []
    with open(fileName) as f:
        lines = f.readlines()
        for i in range(0, len(lines)):
            line = lines[i]
            split = line.rstrip().split(' ')
            if(split[0] == 'Partition'):
                split = split[1].split(',')
                partition = split[0]
                processed += 1
                #containerRealDelayT[Id] += [str(processed)]
                #containerRealDelay[Id] += [str(totalDelay/size)]
                while(counter < len(containerDelayT[Id]) and processed > long(containerDelayT[Id][counter])):
                    counter += 1
                if(counter < len(containerDelayT[Id]) and processed == long(containerDelayT[Id][counter])):
                    counter += 1
                    if(partition not in partitionRealDelay):
                        partitionRealDelay[partition] = []
                        partitionRealDelayT[partition] = []
                        containerPartition[Id] += [partition]
                    partitionRealDelayT[partition] += [containerDelay[Id][counter - 1]]
                    partitionRealDelay[partition] +=[split[1]]
                    containerRealDelayT[Id] += [str(processed)]
                    containerRealDelay[Id] += [split[1]]

                    #print(containerRealDelay)
                    # print(containerRealDelayT)

import numpy as np

#Partition Delay
for Id in containerDelay:
    readContainerRealDelay(Id)
    legend = ['y=x']
    fig = plt.figure(figsize=(25,25))
    xx = np.arange(0., 1000., 10)
    plt.plot(xx , xx,'r-')
    ii = 0
    for partition in containerPartition[Id]:
        legend += ['Partition' + partition]
        plt.plot(partitionRealDelayT[partition], partitionRealDelay[partition], colors[ii%7] + markerType[ii%5])
        ii += 1
    plt.xlabel('Estimate Delay (ms)')
    plt.ylabel('Actual Delay (ms)')
    axes = plt.gca()
    axes.set_ylim([-10,1000])
    axes.set_xlim([-10,1000])
    plt.legend(legend, loc='upper left')
    plt.title('Container ' + Id + ' Delay')
    #plt.show()
    plt.grid(True)
    plt.savefig('figures/Container_'+Id+'_Estimated_Versus_Actual.png')
    plt.close(fig)

for Id in containerDelay:
    readContainerRealDelay(Id)
    legend = ['Estimated Delay', 'Actual Delay']
    fig = plt.figure(figsize=(25,15))
    plt.plot(containerDelayT[Id], containerDelay[Id],'ro', containerRealDelayT[Id], containerRealDelay[Id], 'b^')
    plt.xlabel('X-th Message')
    plt.ylabel('Delay (ms)')
    axes = plt.gca()
    axes.set_ylim([0,1000])
    #axes.set_xlim([-100,600])
    plt.legend(legend, loc='upper left')
    plt.title('Container ' + Id + ' Delay')
    #plt.show()
    plt.grid(True)
    plt.savefig('figures/Container_'+Id+'_Delays.png')
    plt.close(fig)


for Id in containerProcessed:
    legend = ['Arrived', 'Processed']#, 'Processed-Delay']
    fig = plt.figure(figsize=(25,15))
    plt.plot(containerArrivedT[Id], containerArrived[Id],'r^-',containerProcessedT[Id],containerProcessed[Id],'bs-') #, containerDelayT[Id], containerDelay[Id], 'gs-')

    plt.legend(legend, loc='upper left')
    plt.title('Container ' + Id + ' Processed And Arrived')
    #plt.show()
    plt.grid(True)
    plt.savefig('figures/Container_'+Id+'_ProcessedAndArrived.png')
    plt.close(fig)

#print(partitionProcessed)
#print(partitionArrived)
for Id in partitionProcessed:
    legend = ['Arrived', 'Processed']
    fig = plt.figure(figsize=(25,15))
    plt.plot(partitionArrivedT[Id], partitionArrived[Id],'r^-',partitionProcessedT[Id],partitionProcessed[Id],'bs-')
    plt.legend(legend, loc='upper left')
    plt.title('Partition ' + Id + ' Processed And Arrived')
    #plt.show()
    plt.grid(True)
    axes = plt.gca()
    axes.set_ylim([0,15000])
    axes.set_xlim([0,160000])
    plt.savefig('figures/partitions/Partition_'+Id+'_ProcessedAndArrived.png')
    plt.close(fig)
#for Id in containerFlushProcessed:
#plt.plot(containerFlushProcessedT[Id],containerFlushProcessed[Id])
#plt.title('Container ' + Id + ' Flush Processed')
#plt.show()
#
