import os, os.path
#from os.path import splitext, basename, join
import csv
import numpy as np
import networkx as nx
from itertools import combinations
from random import random


def visualize_label_relationship_friend(file_):
    list_json = []
    lable_unique = []
    frequence = []
    edge_data = []
    """
        Từ file tạo :
        + label_unique : chứa danh sách các label là duy nhất
        + edge_data : chứa tất cả các liên kết của 2 label. Có trùng
        + list_json : chứa các dòng của file được đọc lên
    """
    with open (file_, 'r') as csvfile:
        #reader=json.load(file_json)
        #reader=csv.reader(csvfile , delimiter=',', quoting=csv.QUOTE_NONE)
        reader=csv.reader(csvfile)
        for row in reader:
            # Deleted the end element (frequence)
            row_content = row[:len(row) - 1]
            list_json.append(row)
            # Create lable_unique
            for value in row_content:
                if value not in lable_unique:
                    lable_unique.append(value)
            if len(row_content) > 1:
                # Create list all edge in row
                edge_row = list(combinations(row_content, 2))
                edge_data.append(edge_row)


    """
        Tạo:
        + frequence : chứa tần suất xuất hiện của từng label trong data. Index được đánh trùng với label_unique
        + array : là array shape (size, size) (size = len(label_unique)). Ở vị trí array[x][y] chứa số lần xuất
        hiện cùng nhau của 2 label_unique[x] và label_unique[y]
    """
    frequence = [0] * len(lable_unique)
    for i in range(len(lable_unique)):
        for row in list_json:
                freq = row.count(lable_unique[i])
                frequence[i] = frequence[i] + freq * (int)(row[len(row) - 1])

    size = len(lable_unique)
    array = np.zeros((size, size))
    for row in edge_data:
        for edge in row:
            x = lable_unique.index(edge[0])
            y = lable_unique.index(edge[1])
            array[x][y] += 1
            array[y][x] += 1

    friend_edge = (array != 0).sum(0)

    """
        Các option lựa chọn khi vẽ chart
        + scale : nhằm giảm bán kính của all node đi một lương.
        + edge : chỉ in ra các node có số cạnh lớn hơn 10. (tính cả các lần đệ quy)
        + loop : số lần đệ quy
        + font_size : size của label
        + show_label : lựa chọn có hiện thị label hay không
    """
    ################################ OPTION #########################
    # Set node size, scale with node_size = freq / scale
    scale = 10.0
    # Show label, font_size label
    show_label = True
    font_size = 8
    # Conditions for the show node. Numbers of edge
    edge = 10
    loop = 30

    frequence = [(frequence[i] / scale) for i in range(len(lable_unique))]

    # Create a graph
    graph = nx.Graph()
    # Add list node
    graph.add_nodes_from(lable_unique)
    # Add list edge
    for list_edge in edge_data:
        graph.add_edges_from(list_edge)

    #Lọc các node theo option
    n = 0
    list_delete = []
    for j in range(1, loop, 1):
        friend_edge = (array != 0).sum(0)
        for i in range(len(lable_unique)):
            if friend_edge[i] < edge:
                for k in range(len(lable_unique)):
                    array[i][k] = 0
                    array[k][i] = 0
                if lable_unique[i] not in list_delete:
                    list_delete.append(lable_unique[i])
                    frequence[i] = 0
                    n = n + 1
    print ("Numbers node is deleted/Total : %d/%d" %(n, len(lable_unique)))

    frequence_some_node = []
    for i in range(len(frequence)):
        if frequence[i] != 0:
            frequence_some_node.append(frequence[i])
        else:
            graph.remove_node(lable_unique[i])

    # Create color for each node
    colors = [(random()) for _i in range(len(frequence_some_node))]

    nx.draw_spring(graph, node_color=colors, node_size=frequence_some_node, node_shape='o', weight=1, with_labels=show_label, font_size=font_size, font_family='sans-serif')

    import matplotlib.pyplot as plt
    plt.show()

# Run test
# file_='C:/Users/CPU10145-local/Desktop/Python Envirement/Data/Used google cloud API/data_2017-06-23 04_32_17 PM out.csv'
# visualize_label_relationship_friend(file_)
