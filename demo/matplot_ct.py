import matplotlib.pyplot as plt
import numpy as np
import matplotlib as mpl

np.random.seed(1000)
y = np.random.standard_normal((20,2))
x = range(len(y))
# plt.plot(x,y)
#
#
# yAgain = np.random.standard_normal(20)
# plt.plot(x,yAgain)
#
#
# # PPI:
# # Low: 800*600   Middle: 1024*768   High: 1920*1080
# plt.figure(figsize=(8, 6))
# plt.plot(y[:, 0], lw=1.5, label='1st')
# plt.plot(y[:, 1], lw=3, label='2nd')
# # Color:  b, g, r, c, m, y, k, w
# # Line Style: -, _, -., :, ., ,, o, v, ^
# plt.grid(True)
# plt.axis('tight')
# # .axis Parameters:
# # Empty, off, equal, scaled, tight, image
# plt.xlim(-1,30)
# plt.ylim(-3,3)
# plt.xlabel('index')
# plt.ylabel('value')
# plt.title('A Simple Plot')
# legend_loc = {
#     'BestPossible': 0,
#     'UpperRight': 1,
#     'UpperLeft': 2,
#     'LowerLeft': 3,
#     'LowerRight': 4,
#     'Right': 5,
#     'CenterLeft': 6,
#     'CenterRight': 7,
#     'LowerCenter': 8,
#     'UpperCenter': 9,
#     'Center': 10
# }
# plt.legend(loc=legend_loc['Right'])



# PPI:
# Low: 800*600   Middle: 1024*768   High: 1920*1080
subplots_figsize = {
    'Low': (8,6),
    'Middle': (10.24, 7.68),
    'High': (19.20, 10.8)
}
fig, ax1 = plt.subplots(figsize=subplots_figsize['Middle'])
plt.sca(ax=ax1)
plt.plot(y[:, 0], 'g', lw=1.5, label='1st')
plt.plot(y[:, 0], 'bo')
# Color:  b, g, r, c, m, y, k, w
# Line Style: -, _, -., :, ., ,, o, v, ^
plt.grid(True)
plt.axis('tight')
# .axis Parameters:
# Empty, off, equal, scaled, tight, image
plt.xlim(-1,30)
plt.ylim(-3,3)
legend_loc = {
    'BestPossible': 0,
    'UpperRight': 1,
    'UpperLeft': 2,
    'LowerLeft': 3,
    'LowerRight': 4,
    'Right': 5,
    'CenterLeft': 6,
    'CenterRight': 7,
    'LowerCenter': 8,
    'UpperCenter': 9,
    'Center': 10
}
plt.legend(loc=legend_loc['Right'])
plt.xlabel('index')
plt.ylabel('value')
plt.title('A Simple Plot')
ax2 = ax1.twinx()
plt.sca(ax=ax2)
plt.plot(y[:, 1] * 1000, 'r', lw=3, label='2nd')
plt.plot(y[:, 1] * 1000, 'co')
plt.legend(loc=legend_loc['LowerCenter'])
plt.ylabel('value 2nd')



# PPI:
# Low: 800*600   Middle: 1024*768   High: 1920*1080
plt.figure(figsize=(8,6))
plt.subplot(211)
plt.plot(y[:, 0], 'g', lw=1.5, label='1st')
plt.plot(y[:, 0], 'bo')
# Color:  b, g, r, c, m, y, k, w
# Line Style: -, _, -., :, ., ,, o, v, ^
plt.grid(True)
plt.axis('tight')
# .axis Parameters:
# Empty, off, equal, scaled, tight, image
plt.xlim(-1,30)
plt.ylim(-3,3)
legend_loc = {
    'BestPossible': 0,
    'UpperRight': 1,
    'UpperLeft': 2,
    'LowerLeft': 3,
    'LowerRight': 4,
    'Right': 5,
    'CenterLeft': 6,
    'CenterRight': 7,
    'LowerCenter': 8,
    'UpperCenter': 9,
    'Center': 10
}
plt.legend(loc=legend_loc['Right'])
plt.xlabel('index')
plt.ylabel('value')
plt.title('A Simple Plot')
plt.subplot(212)
plt.plot(y[:, 1] * 1000, 'r', lw=3, label='2nd')
plt.plot(y[:, 1] * 1000, 'co')
plt.legend(loc=legend_loc['Right'])
plt.xlabel('index')
plt.ylabel('value 2nd')
plt.title('Second Simple Plot')
plt.axis('tight')
plt.grid(True)



subplots_figsize = {
    'Low': (8,6),
    'Middle': (10.24, 7.68),
    'High': (19.20, 10.8)
}
plt.figure(figsize=subplots_figsize['Middle'])
plt.subplot(121)
plt.plot(y[:, 0], 'g', lw=1.5, label='1st')
plt.plot(y[:, 0], 'bo')
# Color:  b, g, r, c, m, y, k, w
# Line Style: -, _, -., :, ., ,, o, v, ^
plt.grid(True)
plt.axis('tight')
# .axis Parameters:
# Empty, off, equal, scaled, tight, image
plt.xlim(-1,30)
plt.ylim(-3,3)
legend_loc = {
    'BestPossible': 0,
    'UpperRight': 1,
    'UpperLeft': 2,
    'LowerLeft': 3,
    'LowerRight': 4,
    'Right': 5,
    'CenterLeft': 6,
    'CenterRight': 7,
    'LowerCenter': 8,
    'UpperCenter': 9,
    'Center': 10
}
plt.legend(loc=legend_loc['Right'])
plt.xlabel('index')
plt.ylabel('value')
plt.title('A Simple Plot')
plt.subplot(122)
plt.bar(np.arange(len(y)), y[:, 1], width=0.5, color='r', label='2nd')
plt.grid(True)
plt.legend(loc=legend_loc['Right'])
plt.axis('tight')
plt.xlabel('index')
plt.title('Second Simple Plot')




y = np.random.standard_normal((1000, 2))
plt.figure(figsize=subplots_figsize['Middle'])
plt.plot(y[:,0], y[:,1], 'ro')
plt.grid(True)
plt.xlabel('1st')
plt.ylabel('2nd')
plt.title('Scatter Plot')




c = np.random.randint(0, 10, len(y))
plt.figure(figsize=subplots_figsize['Middle'])
plt.scatter(y[:, 0], y[:, 1], c=c, marker='o')
plt.colorbar()
plt.grid(True)
plt.xlabel('1st')
plt.ylabel('2nd')
plt.title('Scatter Plot')







plt.figure(figsize=subplots_figsize['Middle'])
plt.hist(y, label=['1st', '2nd'], bins=25)
plt.grid(True)
plt.legend(loc=legend_loc['Right'])
plt.xlabel('value')
plt.ylabel('frequency')
plt.title('Histogram')



plt.figure(figsize=subplots_figsize['Middle'])
plt.hist(y, label=['1st', '2nd'], color=['r', 'g'], bins=25, stacked=True)
plt.grid(True)
plt.legend(loc=legend_loc['Right'])
plt.xlabel('value')
plt.ylabel('frequency')
plt.title('Histogram')









plt.show()

