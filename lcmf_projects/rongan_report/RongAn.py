import sys
sys.path.append('/home/myGit/lcmf_projects')
sys.path.append('/home/myGit/lcmf_projects/lcmf_projects')
import os
from ipdb import set_trace

import pandas as pd
set_trace()
import database
from ORM_class import rongan_report





os.system('ls ../Data/rongan_report | grep "融安" > name.txt')
os.system('ls ../Data/rongan_report | grep "综合" >> name.txt')

name_list = []
with open('name.txt', mode='r') as file:
    for line in file:
        name_list.append(line.split("\n")[0])


