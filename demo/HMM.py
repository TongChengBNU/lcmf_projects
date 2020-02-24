import pandas as pd
import numpy as np
from ipdb import set_trace

class Box_Ball:
    # initial prob distribution: ini_prob_dis
    # transition prob matrix: tran_prob_mat
    # observation prob matrix: observe_prob_mat

    #----------------------------------------
    # Case description:
    # 4 box: A, B, C, D
    # red:   5 3 6 8
    # white: 5 7 4 2
    #----------------------------------------

    # state space: { 'A', 'B', 'C', 'D' }
    # observation space: { 'red' , 'blue'} = { 0, 1 }
    # length of process:

    len_process = 5
    state_space = {0:'A', 1:'B', 2:'C', 3:'D'}
    # state_space = {'A':0, 'B':1, 'C':2, 'D':3}
    # observation_space = {0:'red', 1:'blue'}
    observation_space = {'red':0, 'blue':1}

    def __init__(self):
        self.ini_prob_dis = [0.25] * 4
        self.tran_prob_mat = np.array([[0,1,0,0],[.4,0,.6,0],[0,.4,0,.6],[0,0,.5,.5]])
        self.observe_prob_mat = np.array([[.5,.5],[.3,.7],[.6,.4],[.8,.2]])
        return

    @staticmethod
    # input: a discrete probability distribution (array)
    # output: index of event
    def random_selection(L):
        cul_dis = np.cumsum(L)

        # uniformly distributed random number
        temp = np.random.rand()

        for i in range(len(cul_dis)):
            if temp <= cul_dis[i]:
                return i




    def generate_series(self):

        # set_trace()
        res_state = []
        res_observe = []
        for i in range(self.len_process):
            if i == 0:
                # initialize
                state = self.random_selection(self.ini_prob_dis)
                res_state.append(state)

                observe = self.random_selection(self.observe_prob_mat[state])
                res_observe.append(observe)

            else:
                index = res_state[-1]
                state = self.random_selection(self.tran_prob_mat[index])
                res_state.append(state)

                observe = self.random_selection(self.observe_prob_mat[state])
                res_observe.append(observe)


        return res_state, res_observe




    def compute_prob_obs(self, obs):
        # observation is a list
        # dim of alpha vector equals to dim of state space
        # iteration num equals to length of 'obs'
        iter_num = len(obs)
        dim = len(self.state_space)

        alpha_container = []
        for j in range(dim):
            alpha_container.append(self.ini_prob_dis[j] * self.observe_prob_mat[j][obs[0]])

        for i in range(iter_num)[1:]:
            alpha_container_copy = alpha_container
            alpha_container = []
            for j in range(dim):
                # please note that the second term of dot product is j-th column, not row.
                temp = (np.array(alpha_container_copy) * self.tran_prob_mat.transpose()[j]).sum()
                alpha_container.append(temp * self.observe_prob_mat[j][obs[i]])



        return np.array(alpha_container).sum()

    def test(self):
        max_test = 1000
        obs_container = dict()
        i = 0

        while len(obs_container) < 32 and i < max_test:
            _, temp_obs = self.generate_series()
            if tuple(temp_obs) not in obs_container.keys():
                obs_container[tuple(temp_obs)] = self.compute_prob_obs(temp_obs)
            i = i+1


        return i, obs_container




if __name__ == "__main__":
    t1 = Box_Ball()
    # state1, obs1 = t1.generate_series()
    # func_prob = t1.compute_prob_obs(obs1)
    iter_index, observe_container = t1.test()

    total_prob = 0
    for _, value in observe_container.items():
        total_prob += value
    print(total_prob)