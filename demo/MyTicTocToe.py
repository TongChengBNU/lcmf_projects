# python 3.5
import math
import random
import pandas as pd
import collections  # for ordered dictionary



class State:
    # initialize a 'State' with 2 para.s: board(list) and turn(unsigned int)
    # all attribute are indicated here
    def __init__(self, board, turn=1):
        self.board = board
        self.turn = turn  # 1 means this turn for cross, and -1 means this turn for circle

    # return board(list) and num of 'turn', but very ugly
    def __str__(self):
        return str(self.board) + str(self.turn)

    # comparative function
    def __eq__(self, other):
        return self.board == other.board and self.turn == other.turn



class Model:
    # 1 for cross, -1 for circle, 0 for null(available position)
    # Describe:
    # Computer1  1     cross       value_table_1
    # Computer2  -1    circle      value_table_2       first play

    CROSS_WIN = 10
    CIRCLE_WIN = -10
    DRAW = 0
    NOT_FINISHED = 2333

    # the default value for new state
    DEFAULT_VALUE = 0.5

    # ‘training_epoch' default = 1000
    def __init__(self, learning_rate, explore_rate, training_epoch=1000):
        self.value_table_1 = {}   # store the map between state and estimated winning probability
        self.value_table_2 = {}   # store the map between state and estimated winning probability

        self.learning_rate = learning_rate # step size when adjusting the probability
        self.explore_rate = explore_rate   # probability of randomly choose next_step

        self.training_epoch = training_epoch
        self.test_result = {'Computer1':0, 'Computer2':0, 'Draw':0}


    @staticmethod
    # static method: not involve the object itself
    def result(state):
        board = state.board
        if (board[0] + board[3] + board[6] == 3
                or board[1] + board[4] + board[7] == 3
                or board[2] + board[5] + board[8] == 3
                or board[0] + board[4] + board[8] == 3
                or board[2] + board[4] + board[6] == 3
                or board[0] + board[1] + board[2] == 3
                or board[3] + board[4] + board[5] == 3
                or board[6] + board[7] + board[8] == 3
        ):
            return Model.CROSS_WIN # return a const

        if (board[0] + board[3] + board[6] == -3
                or board[1] + board[4] + board[7] == -3
                or board[2] + board[5] + board[8] == -3
                or board[0] + board[4] + board[8] == -3
                or board[2] + board[4] + board[6] == -3
                or board[0] + board[1] + board[2] == -3
                or board[3] + board[4] + board[5] == -3
                or board[6] + board[7] + board[8] == -3
        ):
            return Model.CIRCLE_WIN # return a const

        # abs and sum to judge if the board is full
        if sum(map(abs, board)) == 9:
            return Model.DRAW

        # this is an exception, which is designed for emergency
        return Model.NOT_FINISHED

    # return all possible moves for next step (list of all available index)
    @staticmethod
    def next_move_indexes(state):
        next_state_ids = []
        for i in range(len(state.board)):
            if state.board[i] == 0:
                next_state_ids.append(i)
        return next_state_ids

    @staticmethod
    # put 'state.turn' on posi. 'i', return a new state
    def next_state(state, i):
        board = state.board[:]
        board[i] = state.turn
        return State(board, -state.turn)  # converse the mark 'turn'




    # Core functions: explore and exploit

    # find the max in value_table
    def exploit(self, state):
        # select the state with highest (or lowest) value
        next_state_ids = self.next_move_indexes(state)

        # -1 for 'next_step' is an exception
        next_step = -1
        value = -1 # initialize for comparision

        # Case 1: no next step available
        if len(next_state_ids) == 0:
            # next_step = -1 and value = -1 means that no available steps exists.
            return next_step

        # Case 2: search for best nest_step based on current 'knowledge'
        current_turn = state.turn
        if current_turn == 1:
            # Computer1 optimize
            for i in next_state_ids:
                # detect forward
                next_state = self.next_state(state, i)
                key = str(next_state)

                # need to put the best move in the 'value_table_1'
                # select the highest value
                if key in self.value_table_1:
                    if self.value_table_1[key] > value:
                        value = self.value_table_1[key]
                        next_step = i
                # set initial value for states not in value table
                else:
                    self.value_table_1[key] = self.DEFAULT_VALUE  # set initial value of new state
                    if value < 0.5:
                        value = 0.5
                        next_step = i

        else:
            # Computer2 optimize
            for i in next_state_ids:
                # detect forward
                next_state = self.next_state(state, i)
                key = str(next_state)

                # need to put the best move in the 'value_table_2'
                # select the highest value
                if key in self.value_table_2:
                    if self.value_table_2[key] > value:
                        value = self.value_table_2[key]
                        next_step = i
                # set initial value for states not in value table
                else:
                    self.value_table_2[key] = self.DEFAULT_VALUE  # set initial value of new state
                    if value < 0.5:
                        value = 0.5
                        next_step = i



        # if every state doesn't have value in 'value_table', then it will be initialized to 0.5 and return the last one

        return next_step


    # return index of next move
    def explore(self, state):
        # get all states
        next_state_ids = self.next_move_indexes(state)
        next_step = -1
        # no next_step available
        if len(next_state_ids) == 0:
            return next_step

        current_turn = state.turn
        if current_turn == 1:
            # Computer1 turn
            # if state not in value table, set an initial value
            for i in next_state_ids:
                next_state = self.next_state(state, i)

                key = str(next_state)
                if key not in self.value_table_1:
                    self.value_table_1[key] = self.DEFAULT_VALUE
        else:
            # Computer2 turn
            # if state not in value table, set an initial value
            for i in next_state_ids:
                next_state = self.next_state(state, i)

                key = str(next_state)
                if key not in self.value_table_2:
                    self.value_table_2[key] = self.DEFAULT_VALUE

        # select next state randomly, 'random.choice' is a built-in function
        return random.choice(next_state_ids)



    def train(self):
        for i in range(self.training_epoch):
            # get initial state, circle(-1) first
            board = [0 for _ in range(9)]
            board[random.randint(0, 8)] = -1
            state = State(board, 1)

            print("Train game %d: " % i, end="")

            # play one game

            # ordered dictionary could guarantee the order of print
            circle_dic = collections.OrderedDict()
            cross_dic = collections.OrderedDict()

            while True:
                if self.result(state) == Model.CROSS_WIN:
                    # loop over 'cross_dic' and 'circle_dic' to update value_table

                    i = 1
                    for cross_item in cross_dic:
                        # 4 <= length of cross_dic <=5
                        # When cross wins, the prob of 'value_table_1' increases
                        R = 0.5+0.1*i/5
                        self.value_table_1[cross_item] += self.learning_rate*(R - self.value_table_1[cross_item])
                        i += 1

                    i = 1
                    for circle_item in circle_dic:
                        # 4 <= length of circle_dic <=5
                        # When cross wins, the prob of 'value_table_2' decreases
                        R = 0.5-0.1*i/5
                        self.value_table_2[circle_item] += self.learning_rate*(R - self.value_table_2[circle_item])
                        i += 1

                    print("cross win", end=" ")
                    break

                elif self.result(state) == Model.CIRCLE_WIN:


                    i = 1
                    for cross_item in cross_dic:
                        # 4 <= length of cross_dic <=5
                        # When circle wins, the prob of 'value_table_1' reduces
                        R = 0.5 - 0.1 * i / 5
                        self.value_table_1[cross_item] += self.learning_rate * (R - self.value_table_1[cross_item])
                        i += 1

                    i = 1
                    for circle_item in circle_dic:
                        # 4 <= length of circle_dic <=5
                        # When circle wins, the prob of 'value_table_2' increases
                        R = 0.5 + 0.1 * i / 5
                        self.value_table_2[circle_item] += self.learning_rate * (R - self.value_table_2[circle_item])
                        i += 1



                    print("circle win", end=" ")
                    break
                elif self.result(state) == Model.DRAW:
                    # these steps are indifferent, no change occurs
                    print("draw", end=" ")
                    break

                # exploit and explore procedure
                else:
                    print(str(state.board), end=" ")

                    # random.uniform(0,1) is equivalent to np.random.rand()
                    # Explore
                    if random.uniform(0, 1) < self.explore_rate:
                        # prob. of 'explore' is 'explore_rate'

                        next_step = self.explore(state)
                        # when 'next_step' = -1, it means there is no available step to continue.
                        if next_step == -1:
                            break
                        state = self.next_state(state, next_step)

                        # 1 for cross, -1 for circle, 0 for null(available position)
                        if state.turn == -1:  # it means the next turn is -1 and this is a strategy of cross(1)
                            # record the step for later usage
                            cross_dic[str(state)] = 0
                        else:
                            # record the step for later usage
                            circle_dic[str(state)] = 0

                    # Exploit
                    else:
                        next_step= self.exploit(state)
                        # when 'next_step' = -1, it means there is no available step to continue.
                        if next_step == -1:
                            break

                        # non-stationary problem in page 32
                        # self.value_table[str(state)] += self.learning_rate * (value - self.value_table[str(state)])
                        state = self.next_state(state, next_step)

                        # 1 for cross, -1 for circle, 0 for null(available position)
                        if state.turn == -1:  # it means the next turn is -1 and this is a strategy of cross(1)
                            # record the step for later usage
                            cross_dic[str(state)] = 0
                        else:
                            # record the step for later usage
                            circle_dic[str(state)] = 0
            print("")


    # def test(self, test_epochs=10000):
    #     cnt_cross, cnt_circle, cnt_draw = 0, 0, 0
    #     for i in range(test_epochs):
    #         # get initial state, circle first
    #         board = [0 for _ in range(9)]
    #         board[random.randint(0, 8)] = -1
    #         state = State(board, 1)
    #         # self.value_table[str(state)] = 0.5
    #
    #         print("Test game %d: " % i, end="")
    #
    #         while True:
    #             if self.result(state) == Model.CROSS_WIN:
    #                 self.value_table[str(state)] = 1
    #                 print("cross win", end=" ")
    #                 cnt_cross += 1
    #                 break
    #             elif self.result(state) == Model.CIRCLE_WIN:
    #                 self.value_table[str(state)] = 0
    #                 print("circle win", end=" ")
    #                 cnt_circle += 1
    #                 break
    #             elif self.result(state) == Model.DRAW:
    #                 self.value_table[str(state)] = 0.5
    #                 print("draw", end=" ")
    #                 cnt_draw += 1
    #                 break
    #             else:
    #                 print(str(state.board), end=" ")
    #                 next_step = self.exploit(state)
    #                 if next_step == -1:
    #                     break
    #                 state = self.next_state(state, next_step)
    #         print("")
    #     print("Cross win %d, Circle win %d, Draw %d" % (cnt_cross, cnt_circle, cnt_draw))
    #     self.test_result.append((cnt_cross, cnt_circle, cnt_draw))
    #     # print(repr(self.value_table))

    @staticmethod
    def print_state(state):
        board = state.board
        print('------------------------------------------------------')
        print('Current board: \n')
        print(board[0], ' ', board[1], ' ', board[2])
        print(board[3], ' ', board[4], ' ', board[5])
        print(board[6], ' ', board[7], ' ', board[8])
        print('********************************')
        # temp =
        print('Current player: ', state.turn)
        print('------------------------------------------------------\n')
        return




    def play_interactive(self):
        # computer play -1(circle), and you play 1(cross)
        board = [0 for _ in range(9)]
        board[random.randint(0, 8)] = -1
        state = State(board, 1)
        # 1 for cross, -1 for circle, 0 for null(available position)

        while True:
            if self.result(state) == Model.CROSS_WIN:
                self.print_state(state)
                print('You win.\n')
                break
            elif self.result(state) == Model.CIRCLE_WIN:
                self.print_state(state)
                print('Computer win.\n')
                break
            elif self.result(state) == Model.DRAW:
                self.print_state(state)
                print('Draw.\n')
                break
            else:
                if state.turn == 1:
                    self.print_state(state)
                    while True:
                        row, column = eval(input('Please input row and column:  '))
                        next_step = (row - 1) * 3 + column - 1
                        if state.board[next_step] == 0:
                            print('Valid input.\n')
                            break
                        else:
                            print('Invalid input. Please try again.\n')
                    state = self.next_state(state, next_step)

                else:
                    # self.print_state(state)
                    next_step = self.exploit(state)
                    if next_step == -1:
                        break
                    state = self.next_state(state, next_step)


    def play_computer(self, test_epoch=10000):
        stat = {'Computer1':0, 'Computer2':0, 'Draw':0}
        for i in range(test_epoch):
            # Computer2 play -1(circle), and Computer1 play 1(cross)
            # Computer2 play first
            board = [0 for _ in range(9)]
            board[random.randint(0, 8)] = -1
            state = State(board, 1)
            # 1 for cross, -1 for circle, 0 for null(available position)

            while True:
                if self.result(state) == Model.CROSS_WIN:
                    # print('Computer1 win.\n')
                    stat['Computer1'] += 1
                    break
                elif self.result(state) == Model.CIRCLE_WIN:
                    # print('Computer2 win.\n')
                    stat['Computer2'] += 1
                    break
                elif self.result(state) == Model.DRAW:
                    # print('Draw.\n')
                    stat['Draw'] += 1
                    break
                else:
                    if state.turn == 1:
                        next_step = self.exploit(state)
                        if next_step == -1:
                            break
                        state = self.next_state(state, next_step)

                    else:
                        # self.print_state(state)
                        next_step = self.exploit(state)
                        if next_step == -1:
                            break
                        state = self.next_state(state, next_step)

        for item in stat:
            self.test_result[item] = stat[item]


model = Model(learning_rate=0.01, explore_rate=0.2, training_epoch=10000)
model.train()
model.play_computer()

# model.play_interactive()

# print("Start Testing ...")
# model.test(10000)
# model.play_interactive()

# import pandas as pd
# model_policy_ds = pd.Series(model.value_table)
# print(model.test_result)


#------------------------------------------------

ds1 = pd.Series(model.value_table_1)
ds2 = pd.Series(model.value_table_2)

print('The test result of computer combat: ')
print(model.test_result)

print('\n')

print('The abundance of strategy set:')
print('Computer1: ', len(ds1))
print('Computer2: ', len(ds2))

print('\n')

print('Description of Computer1 strategy set: ')
print(ds1.describe())
print('Description of Computer2 strategy set: ')
print(ds2.describe())

# df = pd.DataFrame(pd.Series(model.value_table))
#
# temp = df.reset_index()
# temp.columns = ['state', 'Prob']
# temp['mark'] = 0
#
# def split(x):
#     if x[-2] == '-':
#         return -1
#     else:
#         return 1
#
# temp['mark'] = temp['state'].apply(split)
