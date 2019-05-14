import matplotlib.pyplot as plt
import numpy as np
import pickle
# 从pickle文件读取降维结果
with open("usdata.pickle", "rb") as usdata:
    data = pickle.load(usdata)
    # y = cluster_result[:10000]  # 这里，y表示聚类结果（一维向量，list或者numpy.array都可以）
    # y = np.zeros(5000)
    # y = np.append(y, np.ones(5000), 0)
    y = np.random.randint(0, 5, 10000)
    plt.scatter(data[:, 0], data[:, 1], c=y)
    plt.show()
