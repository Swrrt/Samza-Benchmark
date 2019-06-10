import csv
import numpy as np
import matplotlib.pyplot as plt

containerArrivalRate = {}
containerServiceRate = {}
containerArrivalRateT = {}
containerServiceRateT = {}
containerWindowDelay = {}
containerWindowDelayT = {}
containerRealWindowDelay = {}
containerRealWindowDelayT = {}

newContainerArrivalRate = {}
newContainerServiceRate = {}
newContainerWindowDelay = {}

overallWindowDelay = []
overallRealWindowDelay = []
overallWindowDelayT = []
overallRealWindowDelayT = []

migrationDecisionTime = {}
migrationDeployTime = {}

scalingDecisionTime = {}
scalingDeployTime = {}

sojourn_time_map = {}
esimated_time = {}
ground_truth = {}

initialTime = -1

def parseContainerArrivalRate(split, fw, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = np.long (time)
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
            containerArrivalRateT[Id] += [(np.long (time) - initialTime) / base]
            fw.writerow([(np.long (time) - initialTime) / base, Id, value, '', ''])

def parseContainerServiceRate(split, fw, base):
    global initialTime
    time = split[2]
    if(initialTime == -1):
        initialTime = np.long (time)
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
            containerServiceRateT[Id] += [(np.long (time) - initialTime) / base]
            fw.writerow([(np.long (time) - initialTime) / base, Id, '', value, ''])

def parseContainerWindowDelay(split, fw, base):
    global initialTime, overallWindowDelayT, overallWindowDelay
    time = split[2]
    if(initialTime == -1):
        initialTime = np.long (time)
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
            containerWindowDelayT[Id] += [(np.long (time) - initialTime) / base]
            fw.writerow([(np.long (time) - initialTime) / base, Id, '', '', value])

            if(len(overallWindowDelayT) == 0 or overallWindowDelayT[-1] < (np.long (time) - initialTime)/base):
                overallWindowDelayT += [(np.long (time) - initialTime) / base]
                overallWindowDelay += [float(value)]
            elif(overallWindowDelay[-1] < float(value)):
                overallWindowDelay[-1] = float(value)

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
            lines += [[X, Y, 'y']]

    return lines

def readContainerRealWindowDelay(Id):
    global initialTime, overallWindowDelayT, overallWindowDelay, overallRealWindowDelayT, overallRealWindowDelay
    fileName = "output/container" + Id + ".txt"
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
                time = (np.long (time) - initialTime) / base
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


def draw_arrival_rate(numberOfContainers):
    index = 1
    fig = plt.figure (figsize=(45, 5 * numberOfContainers))
    sortedIds = sorted (newContainerArrivalRate)
    for Id in sortedIds:
        print ('Draw arrival rate: {0} {1}'.format (index, Id))
        plt.subplot (numberOfContainers, 1, index)
        index += 1
        legend = ['Arrival Rate', 'Processing Rate']
        plt.plot (containerArrivalRateT[Id], newContainerArrivalRate[Id], 'r^-', containerServiceRateT[Id],
                  newContainerServiceRate[Id], 'bs-')
        lines = addMigrationLine (Id, 1000)
        for line in lines:
            plt.plot (line[0], line[1], linewidth=3.0, color=line[2])
        plt.legend (legend, loc='upper left')
        plt.xlabel ('Time (ms)')
        plt.ylabel ('Rate (messages per second)')
        plt.title ('Container ' + Id + ' Arrival and Service Rate')
        axes = plt.gca ()
        axes.set_xlim ([0, 450000])
        axes.set_ylim ([0, 1000])
        # plt.show()
        plt.grid (True)
    plt.savefig ('figures/ContainerArrivalAndServiceRate.png')
    plt.close (fig)

def draw_window_delay(numberOfContainers):
    fig = plt.figure (figsize=(45, 5 * numberOfContainers))
    index = 1
    sortedIds = sorted (containerWindowDelay)
    for Id in sortedIds:
        print ('Draw window delay: {0} {1}'.format (index, Id))
        plt.subplot (numberOfContainers, 1, index)
        index += 1
        readContainerRealWindowDelay(Id)
        legend = ['Window Delay', 'Sojourn Time', 'Avg Ground Truth']
        # fig = plt.figure(figsize=(25,15))
        # plt.plot(containerWindowDelayT[Id], containerWindowDelay[Id],'bs', containerRealWindowDelayT[Id], containerRealWindowDelay[Id], 'r^')
        plt.plot(containerWindowDelayT[Id], newContainerWindowDelay[Id],'bs', containerWindowDelayT[Id], sojourn_time_map[Id], 'r^', containerRealWindowDelayT[Id], containerRealWindowDelay[Id], 'g^')
        # plt.plot(containerWindowDelayT[Id], containerWindowDelay[Id],'bs')
        lines = addMigrationLine (Id, 50)
        for line in lines:
            plt.plot (line[0], line[1], linewidth=3.0, color=line[2])
        plt.legend (legend, loc='upper left')
        plt.xlabel ('Time (ms)')
        plt.ylabel ('Delay (ms)')
        axes = plt.gca ()
        axes.set_xlim ([0, 450000])
        axes.set_ylim ([0, 50])
        plt.title ('Container ' + Id + ' Window Delay')
        # plt.show()
        plt.grid (True)
    plt.savefig ('figures/ContainerWindowDelay.png')
    plt.close (fig)

def draw_graph():
    numberOfContainers = len (newContainerArrivalRate)
    draw_arrival_rate(numberOfContainers)
    draw_window_delay(numberOfContainers)

def compute_sojourn_time():
    sortedIds = sorted (newContainerArrivalRate)
    for Id in sortedIds:
        for arrival_rate, service_rate  in zip(newContainerArrivalRate[Id], newContainerServiceRate[Id]):
            print(arrival_rate, service_rate)
            if service_rate == arrival_rate:
                sojourn_time = 0
            else:
                sojourn_time = 1 / float(service_rate - arrival_rate)*1000
            if Id not in sojourn_time_map:
                sojourn_time_map[Id] = []
            sojourn_time_map[Id].append(sojourn_time)

def float_converter(list, new_list):
    for key in list:
        new_list[key] = [float(i) for i in list[key]]

def comparison():
    input_file = "output/log.txt"
    output_file = "windowno.csv"

    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
    lineType = ['--', '-.', ':']
    markerType = ['+', 'o', 'd', '*', 'x']
    schema = ['Time', 'Container Id', 'Arrival Rate', 'Service Rate', 'Sojourn Time', 'Window Estimate Delay',
              'Ground Truth']

    with open (output_file, 'w') as csvfile:
        fw = csv.writer (csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        fw.writerow (schema)
        print ("Reading from file:" + input_file)
        counter = 0
        arrived = {}
        base = 1
        with open (input_file) as f:
            lines = f.readlines ()
            for i in range (0, len (lines)):
                line = lines[i]
                split = line.rstrip ().split (' ')
                counter += 1
                if (counter % 100 == 0):
                    print ("Current progress to line:" + str (counter))
                if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Arrival' and split[5] == 'Rate:'):
                    parseContainerArrivalRate (split, fw, base)
                if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Service' and split[5] == 'Rate:'):
                    parseContainerServiceRate (split, fw, base)
                if (split[0] == 'MixedLoadBalanceManager,' and split[4] == 'Average' and split[5] == 'Delay:'):
                    parseContainerWindowDelay (split, fw, base)
                    # Add migration marker
                    if (split[0] == 'MigratingOnceBalancer:' and split[2] == 'best' and split[-1] != '[]'):
                        src = split[9]
                        tgt = split[12][:-1]
                        time = lines[i + 1].rstrip ().split (' ')[2]
                        time = (np.long (time) - initialTime) / base
                        if (src not in migrationDecisionTime):
                            migrationDecisionTime[src] = []
                        migrationDecisionTime[src] += [time]
                        if (tgt not in migrationDecisionTime):
                            migrationDecisionTime[tgt] = []
                        migrationDecisionTime[tgt] += [-time]
                        print (src + ' !!! ' + tgt)

                    if (split[0] == 'MigrateLargestByNumberScaler:' and split[3] == 'scale' and split[4] == 'out'):
                        src = split[-1]
                        time = lines[i + 3].rstrip ().split (' ')[2]
                        time = (np.long (time) - initialTime) / base
                        tgt = str (len (lines[i + 3].rstrip ().split (' ')) - 5 + 1).zfill (6)
                        if (src not in migrationDecisionTime):
                            migrationDecisionTime[src] = []
                        migrationDecisionTime[src] += [time]
                        if (tgt not in migrationDecisionTime):
                            migrationDecisionTime[tgt] = []
                        migrationDecisionTime[tgt] += [-time]

                    if (split[0] == 'MixedLoadBalanceManager,' and split[5] == 'deployed!' and split[6] != 'task'):
                        src = split[8][:6]
                        time = split[2]
                        time = (np.long (time) - initialTime) / base
                        if (src not in migrationDeployTime):
                            migrationDeployTime[src] = []
                        migrationDeployTime[src] += [time]

                    if (split[0] == 'MixedLoadBalanceManager,' and split[5] == 'deployed!' and split[6] == 'task'):
                        tgt = split[-1][:6]
                        time = split[2]
                        time = (np.long (time) - initialTime) / base
                        if (tgt not in migrationDeployTime):
                            migrationDeployTime[tgt] = []
                        migrationDeployTime[tgt] += [-time]

        float_converter(containerArrivalRate, newContainerArrivalRate)
        float_converter(containerServiceRate, newContainerServiceRate)
        float_converter(containerWindowDelay, newContainerWindowDelay)
        compute_sojourn_time()
        draw_graph()

if __name__ == "__main__":
    comparison()