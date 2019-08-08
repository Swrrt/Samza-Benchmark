import matplotlib.pyplot as plt
import json
import numpy as np
from itertools import zip_longest

from IPython.core.pylabtools import figsize


class container_metrics_class:
    def __init__(self, containerId):
        self.containerId = containerId
        self.service_rate_list = []
        self.utilization_list = []
        self.average_latency_list = []

    def add_metrics(self, service_rate, utilization, average_latency):
        if type(service_rate) is float:
            self.service_rate_list.append(service_rate)
        else:
            self.service_rate_list.append(service_rate[1])
        self.utilization_list.append(utilization)
        self.average_latency_list.append(average_latency)

    def get_service_rate_list(self):
        return self.service_rate_list

    def get_utilization_list(self):
        return self.utilization_list

    def get_average_latency_list(self):
        return self.average_latency_list


def cdf(data):
    n = len(data)
    x = np.sort(data) # sort your data
    y = np.arange(1, n + 1) / n # calculate cumulative probability
    return x, y

def line(data):
    x = []
    n = len(data)
    cur = 0
    for i in range(0,n):
        x.append(cur)
        cur = cur + 5
    return x, data

def get_metrics(file_name):
    # fp = open("/home/myc/workspace/" + file_name + ".json")
    fp = open("/home/myc/workspace/cs5101-experiment/" + file_name + ".json")
    container_map = {}

    # service_rate_list = []
    # utilization_list = []4
    # average_latency_list = [
    text = fp.readline()
    count = 0
    while text:
        json_data = json.loads (text)
        if "org.apache.samza.container.SamzaContainerMetrics" in json_data["metrics"][1] :
            container_name = json_data["header"][1]["exec-env-container-id"]
            service_rate = json_data["metrics"][1]["org.apache.samza.container.SamzaContainerMetrics"][1]["service-rate"]
            average_utilization = json_data["metrics"][1]["org.apache.samza.container.SamzaContainerMetrics"][1]["average-utilization"]
            average_latency = json_data["metrics"][1]["org.apache.samza.container.SamzaContainerMetrics"][1]["average-latency"][1]
            if container_name not in container_map:
                container_metrics = container_metrics_class(container_name)
                container_map[container_name] = container_metrics
            container_map[container_name].add_metrics(service_rate, average_utilization, average_latency)
        text = fp.readline()

    app_service_rate_list = {}
    app_average_latency_list = {}

    for key in container_map:
        stage_attempt = key[:-7]
        if stage_attempt not in app_service_rate_list:
            app_service_rate_list[stage_attempt] = []
            app_average_latency_list[stage_attempt] = []
        app_service_rate_list[stage_attempt].append(container_map[key].service_rate_list)
        app_average_latency_list[stage_attempt].append(container_map[key].average_latency_list)

    service_rate_list = []
    average_latency_list = []
    for key in app_service_rate_list:
        stage_service_rate_list = [sum(x) for x in zip_longest(*app_service_rate_list[key], fillvalue=0)]
        stage_average_latency_list = [sum(x)/len(x) for x in zip_longest(*app_average_latency_list[key], fillvalue=0)]
        service_rate_list.append(stage_service_rate_list)
        average_latency_list.append(stage_average_latency_list)


    # wordcount
    if len(service_rate_list) > 1:
        actual_service_rate_list = [max(x)/25 for x in zip_longest(*service_rate_list, fillvalue=0)]
        actual_average_latency_list = [sum(x) for x in zip_longest(*average_latency_list, fillvalue=0)]
        return actual_service_rate_list, actual_average_latency_list
    else:
        actual_service_rate_list = service_rate_list[0]
        actual_average_latency_list = average_latency_list[0]
        return actual_service_rate_list, actual_average_latency_list

    # wikipedia
    # if len(service_rate_list) > 1:
    #     actual_service_rate_list = [max(x) for x in zip_longest(*service_rate_list, fillvalue=0)]
    #     # actual_average_latency_list = [sum(x) for x in zip_longest(*average_latency_list, fillvalue=0)]
    #     latter_stage_average_latency_list = [max(x) for x in zip_longest(*average_latency_list, fillvalue=0)]
    #     former_stage_average_latency_list = [min(x) for x in zip_longest(*average_latency_list, fillvalue=0)]
    #     actual_average_latency_list = [i + j for i, j in zip(latter_stage_average_latency_list, former_stage_average_latency_list)]
    #     return actual_service_rate_list, actual_average_latency_list
    # else:
    #     actual_service_rate_list = service_rate_list[0]
    #     actual_average_latency_list = average_latency_list[0]
    #     return actual_service_rate_list, actual_average_latency_list

if __name__ == "__main__":

    # # wordcount
    # # exp1
    s_app_service_rate_list, s_app_average_latency_list = get_metrics("exp-wc/metrics-s-2c")
    app_service_rate_list, app_average_latency_list = get_metrics("exp-wc-nos/metrics-1c")

    plt.figure(figsize(4.5,6))
    plt.subplot(2, 1, 1)
    # x_srl, y_srl = cdf(app_service_rate_list)
    # s_x_srl, s_y_srl = cdf(s_app_service_rate_list)
    x_srl, y_srl = line(app_service_rate_list)
    s_x_srl, s_y_srl = line(s_app_service_rate_list)

    plt.plot(x_srl, y_srl, "r*-", s_x_srl, s_y_srl, "b*-")
    axes = plt.gca ()
    # axes.set_xlim ([200, 950])
    plt.xlabel('The maximum throughput(tuples/s)')
    plt.ylabel('Cumulative percent')
    legend = ['s-1c', 'm-2c']
    plt.legend (legend, loc='lower right')
    plt.subplot(2, 1, 2)
    # x_all, y_all = cdf(app_average_latency_list)
    # s_x_all, s_y_all = cdf(s_app_average_latency_list)
    x_all, y_all = line(app_average_latency_list)
    s_x_all, s_y_all = line(s_app_average_latency_list)
    plt.plot(x_all, y_all, "r^-", s_x_all, s_y_all, "b^-")
    plt.xlabel('Aaverage end-to-end latency(ms)')
    plt.ylabel('Cumulative percent')
    legend = ['s-1c', 'm-2c']
    plt.legend (legend, loc='lower right')
    axes = plt.gca ()
    # axes.set_xlim ([0, 100])
    axes.set_ylim ([0, 100])
    plt.show()
    # plt.savefig ('figures/wc-1p.png')
    #
    # # #exp2
    # plt.figure(figsize(4.5,6))
    # s_app_service_rate_list, s_app_average_latency_list = get_metrics("exp-wc/metrics-s-6c")
    # app_service_rate_list, app_average_latency_list = get_metrics("exp-wc-nos/metrics-5c-2")
    #
    # plt.subplot(2, 1, 1)
    # x_srl, y_srl = cdf(app_service_rate_list)
    # s_x_srl, s_y_srl = cdf(s_app_service_rate_list)
    # plt.plot(x_srl, y_srl, "r*", s_x_srl, s_y_srl, "b*")
    # axes = plt.gca ()
    # # axes.set_xlim ([200, 950])
    # plt.xlabel('The maximum throughput(tuples/s)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-5c', 'm-6c']
    # plt.legend (legend, loc='lower right')
    # plt.subplot(2, 1, 2)
    # x_all, y_all = cdf(app_average_latency_list)
    # s_x_all, s_y_all = cdf(s_app_average_latency_list)
    # plt.plot(x_all, y_all, "r^", s_x_all, s_y_all, "b^")
    # plt.xlabel('Aaverage end-to-end latency(ms)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-5c', 'm-6c']
    # plt.legend (legend, loc='lower right')
    # axes = plt.gca ()
    # # axes.set_xlim ([0, 100])
    # # plt.show()
    # plt.savefig ('figures/wc-5p.png')
    #
    # # #exp3
    # plt.figure(figsize(4.5,6))
    # s_app_service_rate_list, s_app_average_latency_list = get_metrics("exp-wc/metrics-s-11c")
    # app_service_rate_list, app_average_latency_list = get_metrics("exp-wc-nos/metrics-10c-2")
    # plt.subplot(2, 1, 1)
    # x_srl, y_srl = cdf(app_service_rate_list)
    # s_x_srl, s_y_srl = cdf(s_app_service_rate_list)
    # plt.plot(x_srl, y_srl, "r*", s_x_srl, s_y_srl, "b*")
    # axes = plt.gca ()
    # # axes.set_xlim ([200, 950])
    # plt.xlabel('The maximum throughput(tuples/s)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-10c', 'm-11c']
    # plt.legend (legend, loc='lower right')
    # plt.subplot(2, 1, 2)
    # x_all, y_all = cdf(app_average_latency_list)
    # s_x_all, s_y_all = cdf(s_app_average_latency_list)
    # plt.plot(x_all, y_all, "r^", s_x_all, s_y_all, "b^")
    # plt.xlabel('Aaverage end-to-end latency(ms)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-10c', 'm-11c']
    # plt.legend (legend, loc='lower right')
    # axes = plt.gca ()
    # axes.set_xlim ([0, 100])
    # # plt.show()
    # plt.savefig ('figures/wc-10p.png')

    #wikipedia
    # exp1
    # s_app_service_rate_list, s_app_average_latency_list = get_metrics("exp-wiki/metrics-wiki-s-4c")
    # app_service_rate_list, app_average_latency_list = get_metrics("exp-wiki-nos/metrics-wiki-1c")
    # plt.figure(figsize(4.5,6))
    # plt.subplot(2, 1, 1)
    # x_srl, y_srl = cdf(app_service_rate_list)
    # s_x_srl, s_y_srl = cdf(s_app_service_rate_list)
    # plt.plot(x_srl, y_srl, "r*", s_x_srl, s_y_srl, "b*")
    # axes = plt.gca ()
    # # axes.set_xlim ([200, 950])
    # plt.xlabel('The maximum throughput(tuples/s)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-1c', 'm-2c']
    # plt.legend (legend, loc='lower right')
    # plt.subplot(2, 1, 2)
    # x_all, y_all = cdf(app_average_latency_list)
    # s_x_all, s_y_all = cdf(s_app_average_latency_list)
    # plt.plot(x_all, y_all, "r^", s_x_all, s_y_all, "b^")
    # plt.xlabel('Aaverage end-to-end latency(ms)')
    # plt.ylabel('Cumulative percent')
    # axes = plt.gca ()
    # axes.set_xlim ([0, 100])
    # legend = ['s-1c', 'm-2c']
    # plt.legend (legend, loc='lower right')
    # # plt.show()
    # plt.savefig ('figures/wiki-1p-2.png')

    # # #exp2
    # fig = plt.figure(figsize(4.5,6))
    # s_app_service_rate_list, s_app_average_latency_list = get_metrics("exp-wiki/metrics-wiki-s-8c")
    # app_service_rate_list, app_average_latency_list = get_metrics("exp-wiki-nos/metrics-wiki-5c")
    #
    # plt.subplot(2, 1, 1)
    # x_srl, y_srl = cdf(app_service_rate_list)
    # s_x_srl, s_y_srl = cdf(s_app_service_rate_list)
    # plt.plot(x_srl, y_srl, "r*", s_x_srl, s_y_srl, "b*")
    # axes = plt.gca ()
    # # axes.set_xlim ([200, 950])
    # plt.xlabel('The maximum throughput(tuples/s)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-5c', 'm-7c']
    # plt.legend (legend, loc='lower right')
    # plt.subplot(2, 1, 2)
    # x_all, y_all = cdf(app_average_latency_list)
    # s_x_all, s_y_all = cdf(s_app_average_latency_list)
    # plt.plot(x_all, y_all, "r^", s_x_all, s_y_all, "b^")
    # plt.xlabel('Aaverage end-to-end latency(ms)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-5c', 'm-7c']
    # plt.legend (legend, loc='lower right')
    # axes = plt.gca ()
    # axes.set_xlim ([0, 100])
    # # plt.show()
    # plt.savefig ('figures/wiki-5p-2.png')
    # plt.close (fig)
    #
    # # #exp3
    # s_app_service_rate_list, s_app_average_latency_list = get_metrics("exp-wiki/metrics-wiki-s-13c")
    # app_service_rate_list, app_average_latency_list = get_metrics("exp-wiki-nos/metrics-wiki-10c")
    #
    # plt.subplot(2, 1, 1)
    # x_srl, y_srl = cdf(app_service_rate_list)
    # s_x_srl, s_y_srl = cdf(s_app_service_rate_list)
    # plt.plot(x_srl, y_srl, "r*", s_x_srl, s_y_srl, "b*")
    # axes = plt.gca ()
    # # axes.set_xlim ([200, 950])
    # plt.xlabel('The maximum throughput(tuples/s)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-10c', 'm-13c']
    # plt.legend (legend, loc='lower right')
    #
    # plt.subplot(2, 1, 2)
    # x_all, y_all = cdf(app_average_latency_list)
    # s_x_all, s_y_all = cdf(s_app_average_latency_list)
    # plt.plot(x_all, y_all, "r^", s_x_all, s_y_all, "b^")
    # plt.xlabel('Aaverage end-to-end latency(ms)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-10c', 'm-13c']
    # plt.legend (legend, loc='lower right')
    # axes = plt.gca ()
    # axes.set_xlim ([0, 100])
    # # plt.show()
    # plt.savefig ('figures/wiki-10p.png')

    # stock analysis
    # exp1
    # fig = plt.figure(figsize(4.5,6))
    # s_app_service_rate_list, s_app_average_latency_list = get_metrics("exp-stock/metrics-stock-2c")
    # app_service_rate_list, app_average_latency_list = get_metrics("exp-stock/metrics-stock-nos-1c")
    # plt.subplot(2, 1, 1)
    # x_srl, y_srl = cdf(app_service_rate_list)
    # s_x_srl, s_y_srl = cdf(s_app_service_rate_list)
    # plt.plot(x_srl, y_srl, "r*", s_x_srl, s_y_srl, "b*")
    # axes = plt.gca ()
    # # axes.set_xlim ([200, 950])
    # plt.xlabel('The maximum throughput(tuples/s)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-1c', 'm-2c']
    # plt.legend (legend, loc='lower right')
    # plt.subplot(2, 1, 2)
    # x_all, y_all = cdf(app_average_latency_list)
    # s_x_all, s_y_all = cdf(s_app_average_latency_list)
    # plt.plot(x_all, y_all, "r^", s_x_all, s_y_all, "b^")
    # plt.xlabel('Aaverage end-to-end latency(ms)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-1c', 'm-2c']
    # plt.legend (legend, loc='lower right')
    # axes = plt.gca ()
    # axes.set_xlim ([0, 100])
    # # plt.show()
    # plt.savefig ('figures/stock-1p.png')
    # plt.close (fig)
    #
    #
    # # exp3
    # fig = plt.figure(figsize(4.5,6))
    # s_app_service_rate_list, s_app_average_latency_list = get_metrics("exp-stock/metrics-stock-4c")
    # app_service_rate_list, app_average_latency_list = get_metrics("exp-stock/metrics-stock-nos-4c")
    # plt.subplot(2, 1, 1)
    # x_srl, y_srl = cdf(app_service_rate_list)
    # s_x_srl, s_y_srl = cdf(s_app_service_rate_list)
    # plt.plot(x_srl, y_srl, "r*", s_x_srl, s_y_srl, "b*")
    # axes = plt.gca ()
    # # axes.set_xlim ([200, 950])
    # plt.xlabel('The maximum throughput(tuples/s)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-4c', 'm-4c']
    # plt.legend (legend, loc='lower right')
    # plt.subplot(2, 1, 2)
    # x_all, y_all = cdf(app_average_latency_list)
    # s_x_all, s_y_all = cdf(s_app_average_latency_list)
    # plt.plot(x_all, y_all, "r^", s_x_all, s_y_all, "b^")
    # plt.xlabel('Aaverage end-to-end latency(ms)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-4c', 'm-4c']
    # plt.legend (legend, loc='lower right')
    # axes = plt.gca ()
    # axes.set_xlim ([0, 100])
    # # plt.show()
    # plt.savefig ('figures/stock-4p.png')
    # plt.close (fig)
    #
    # # exp3
    # fig = plt.figure(figsize(4.5,6))
    # s_app_service_rate_list, s_app_average_latency_list = get_metrics("exp-stock/metrics-stock-8c")
    # app_service_rate_list, app_average_latency_list = get_metrics("exp-stock/metrics-stock-nos-7c")
    # plt.subplot(2, 1, 1)
    # x_srl, y_srl = cdf(app_service_rate_list)
    # s_x_srl, s_y_srl = cdf(s_app_service_rate_list)
    # plt.plot(x_srl, y_srl, "r*", s_x_srl, s_y_srl, "b*")
    # axes = plt.gca ()
    # # axes.set_xlim ([200, 950])
    # plt.xlabel('The maximum throughput(tuples/s)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-8c', 'm-8c']
    # plt.legend (legend, loc='lower right')
    # plt.subplot(2, 1, 2)
    # x_all, y_all = cdf(app_average_latency_list)
    # s_x_all, s_y_all = cdf(s_app_average_latency_list)
    # plt.plot(x_all, y_all, "r^", s_x_all, s_y_all, "b^")
    # plt.xlabel('Aaverage end-to-end latency(ms)')
    # plt.ylabel('Cumulative percent')
    # legend = ['s-8c', 'm-8c']
    # plt.legend (legend, loc='lower right')
    # axes = plt.gca ()
    # axes.set_xlim ([0, 100])
    # # plt.show()
    # plt.savefig ('figures/stock-8p.png')
    # plt.close (fig)

    # plot the runtime of the application

    # plt.subplot(2, 1, 1)
    # plt.plot(s_app_service_rate_list)
    # plt.ylabel('Service Rate(tuple/s)')
    #
    # plt.subplot(2, 1, 2)
    # plt.plot(s_app_average_latency_list)
    # plt.xlabel('time (100ms)')
    # axes = plt.gca ()
    # axes.set_ylim ([0, 100])
    # plt.ylabel('Avg latency(ms)')
    #
    # axes = plt.gca ()
    # plt.show()
