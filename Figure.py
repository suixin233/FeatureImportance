import numpy as np
import os
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import random

# 加载数据
RSR_df = pd.read_csv("RSR_data.csv")
RSR_score = np.loadtxt("RSR_score.txt")
RSR_error = np.loadtxt("RSR_error.txt")
LS_score = np.loadtxt("LS_score.txt")
feature_number = RSR_score.size
sample_number = RSR_error.size
LS_fn = LS_score.size

# 画图
f, ax = plt.subplots(figsize = (14, 10))
plt.xticks(np.arange(0, 200, 50))
# 设置颜色
cmap = sns.cubehelix_palette(start = 1.5, rot = 3, gamma=0.8, as_cmap = True)
# 热力图
# cmap用cubehelix map颜色
# sns.heatmap(RSR_df,  linewidths = 0.02, vmin=0, vmax=1, ax=ax, cmap=cmap, xticklabels=False)
# cmap用YlGnBu
sns.heatmap(RSR_df,  linewidths = 0.01, vmin=0, vmax=1, ax=ax, cmap="YlGnBu")
# 设置Axes的标题
ax.set_title("Correlation between features", fontsize=23, position=(0.5,1.05))
# 将y轴或x轴进行逆序
ax.invert_yaxis()

ax.set_xlabel("Features",fontsize=20)
# 设置Y轴标签的字体大小和字体颜色
ax.set_ylabel("Features",fontsize=20, color="r")
# 设置坐标轴刻度的字体大小
# matplotlib.axes.Axes.tick_params
ax.tick_params(axis="x",labelsize=12, labeltop=True, labelbottom=False) # x轴
ax.tick_params(axis="y",labelsize=12) # y轴
# 将x轴刻度放置在top位置的几种方法
ax.xaxis.tick_top()
# 旋转轴刻度上文字方向的两种方法
ax.set_xticklabels(ax.get_xticklabels(), rotation=-90)
# 保存图片
f.savefig("RSR_heatmap.png", dpi=100, bbox_inches="tight")

plt.figure(2)
bar_width = 0.3
plt.bar(np.arange(feature_number), list(RSR_score.data), width=0.3, color='y')
plt.xticks(fontsize=20)
plt.yticks(fontsize=20)
plt.xlabel("Features", fontsize=24)
plt.ylabel("Scores", fontsize=24)

plt.figure(3)
plt.plot(np.arange(sample_number), list(RSR_error.data), marker='*')
plt.xticks(fontsize=20)
plt.yticks(fontsize=20)
plt.xlabel("Samples", fontsize=24)
plt.ylabel("Errors", fontsize=24)

plt.figure(4)
bar_width = 0.3
plt.bar(np.arange(LS_fn), list(LS_score.data), width=0.5, color='y')
plt.xticks(fontsize=20)
plt.yticks(fontsize=20)
plt.xlabel("Features", fontsize=24)
plt.ylabel("Scores", fontsize=24)

plt.show()