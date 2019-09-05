# python 3.5
import math
import random

# 1 for cross, -1 for circle, 0 for null(available position)

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
    CROSS_WIN = 1
    CIRCLE_WIN = -1
    DRAW = 0
    NOT_FINISHED = 2333

    # ‘training_epoch' default = 1000
    def __init__(self, learning_rate, explore_rate, training_epoch=1000):
        self.value_table = {}   # ???? what for, type dict
        self.learning_rate = learning_rate
        self.explore_rate = explore_rate
        self.training_epoch = training_epoch
        pass

    @staticmethod
    # static method: not involve the object itself
    def result(state):
        # ???????? state has board?
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
        if len(next_state_ids) == 0:
            return -1, -1

        # -1 for 'next_state' is an exception

        if state.turn == 1:
            # cross turn, select the highest
            next_step = -1
            value = -math.inf
            for i in next_state_ids:
                next_state = self.next_state(state, i)
                key = str(next_state)

                # select the highest value
                if key in self.value_table:
                    if self.value_table[key] > value:
                        value = self.value_table[key]
                        next_step = i
                # set initial value for states not in value table
                else:
                    self.value_table[key] = 0.5     # set initial value of new state
                    if next_step == -1:
                        value = 0.5
                        next_step = i
            return next_step, value

        elif state.turn == -1:
            # circle turn, select lowest
            next_step = -1
            value = math.inf
            for i in next_state_ids:
                next_state = self.next_state(state, i)
                key = str(next_state)

                # select the lowest value
                if key in self.value_table:
                    if self.value_table[key] < value:
                        value = self.value_table[key]
                        next_step = i
                # set initial value for states not in value table
                else:
                    self.value_table[key] = 0.5  # set initial value of new state
                    if next_step == -1:
                        value = 0.5
                        next_step = i
            return next_step, value

    # return index of next move
    def explore(self, state):
        # get all states
        next_state_ids = self.next_move_indexes(state)
        if len(next_state_ids) == 0:
            return -1, -1

        # if state not in value table, set an initial value
        for i in next_state_ids:
            next_state = self.next_state(state, i)

            key = str(next_state)
            if key not in self.value_table:
                self.value_table[key] = 0.5

        # select next state randomly, 'random.choice' is a built-in function
        return random.choice(next_state_ids)



    def train(self):
        for i in range(self.training_epoch):
            # get initial state, circle first
            board = [0 for _ in range(9)]
            board[random.randint(0, 8)] = -1
            state = State(board, 1)
            # every state corresponds to a number
            self.value_table[str(state)] = 0.5

            # i substitute d and ending with ""
            print("Train game %d: " % i, end="")

            # play one game
            while True:
                if self.result(state) == Model.CROSS_WIN:
                    self.value_table[str(state)] = 1
                    print("cross win", end=" ")
                    break
                elif self.result(state) == Model.CIRCLE_WIN:
                    self.value_table[str(state)] = 0
                    print("circle win", end=" ")
                    break
                elif self.result(state) == Model.DRAW:
                    self.value_table[str(state)] = 0.5
                    print("draw", end=" ")
                    break
                else:
                    print(str(state.board), end=" ")
                    # random.uniform(0,1) is equivalent to np.random.rand()
                    if random.uniform(0, 1) < self.explore_rate:
                        # prob. of 'explore' is 'explore_rate'
                        next_step = self.explore(state)
                        # when 'next_step' = -1, it means there is no available step to continue.
                        if next_step == -1:
                            break
                        state = self.next_state(state, next_step)
                    else:
                        next_step, value = self.exploit(state)
                        # when 'next_step' = -1, it means there is no available step to continue.
                        if next_step == -1:
                            break
                        # non-stationary problem in page 32
                        self.value_table[str(state)] += self.learning_rate * (value - self.value_table[str(state)])
                        state = self.next_state(state, next_step)
            print("")

    def test(self, test_epochs=10000):
        cnt_cross, cnt_circle, cnt_draw = 0, 0, 0
        for i in range(test_epochs):
            # get initial state, circle first
            board = [0 for _ in range(9)]
            board[random.randint(0, 8)] = -1
            state = State(board, 1)
            self.value_table[str(state)] = 0.5

            print("Test game %d: " % i, end="")

            while True:
                if self.result(state) == Model.CROSS_WIN:
                    self.value_table[str(state)] = 1
                    print("cross win", end=" ")
                    cnt_cross += 1
                    break
                elif self.result(state) == Model.CIRCLE_WIN:
                    self.value_table[str(state)] = 0
                    print("circle win", end=" ")
                    cnt_circle += 1
                    break
                elif self.result(state) == Model.DRAW:
                    self.value_table[str(state)] = 0.5
                    print("draw", end=" ")
                    cnt_draw += 1
                    break
                else:
                    print(str(state.board), end=" ")
                    next_step, value = self.exploit(state)
                    if next_step == -1:
                        break
                    state = self.next_state(state, next_step)
            print("")
        print("Cross win %d, Circle win %d, Draw %d" % (cnt_cross, cnt_circle, cnt_draw))
        print(repr(self.value_table))


model = Model(learning_rate=0.01, explore_rate=0.2, training_epoch=1000)
model.train()
print("Start Testing ...")
model.test(10000)
