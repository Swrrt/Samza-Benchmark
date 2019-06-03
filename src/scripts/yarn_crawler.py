import urllib.request
import re


def get_ground_truth(url, name):
    page = urllib.request.urlopen(url)
    html = page.read().decode('utf-8')
    # save html to local .html
    file = open("../data/analysis/"+name+".html", "w+")
    file.write(html)
    file.close()
    ground_truth_arr = re.findall(r'(Partition.*),(.*)\n', html)
    return ground_truth_arr

def sliding_window_ground_truth(ground_truth_arr):
   print(ground_truth_arr)


if __name__ == "__main__":
    suffix = "/stdout/?start=0"
    app_url = "http://localhost:8088/cluster/appattempt/appattempt_1559218445709_0002_000001"
    response = urllib.request.urlopen(app_url)
    # parse html
    html = response.read().decode('utf-8')

    # find container number in this url
    linkregex = re.compile('<a\s*href=.*http://.*containerlogs/.*?>')
    container_regex = re.compile('container_.*\/')

    links = linkregex.findall(html)
    for link in links:
        cleaned_url = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', link)[1][:-2]
        container_name = container_regex.findall(cleaned_url)[0][:-1]
        if container_name[-1] == "1":
            continue
        container_url = cleaned_url + suffix
        ground_truth_arr = get_ground_truth(container_url, container_name)
        # sliding window
        sliding_window_ground_truth(ground_truth_arr)
