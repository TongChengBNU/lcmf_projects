#!bin/bash/

#python main.py main --path ./Output/NewFourTotal_4.csv --sortKey F_PRT_BONDTOTOT
#python main.py main --path ./Output/NewFourFour_4.csv --sortKey F_PRT_FOUR

# store data locally
#python main-2.py main

nohup python main.py crossdata --date '2018-01-31' &
