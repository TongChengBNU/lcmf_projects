'''
40 students, 4 interest classes
every two students share at least one course;
what is the minimum number of the class with the largest members in all possible cases?

'''

def dive(total: int, batch: int):
    if total < batch:
        return []

    res = []
    if batch == 1:
        for i in range(total)[::-1]:
            res.append([i])
        return res
    else:
        for i in range(total)[::-1]:
            tmp_container = dive(total=i, batch=batch-1)
            for sub_slice in tmp_container:
                res.append(sub_slice + [i])
        return res

# test1 = dive(3,2)
# test2 = dive(5,3)

meta_index = list(range(3,41))
index_container = dive(len(meta_index), 4)
value_list = list(map(lambda x: x*(x-1)//2, meta_index))
def find(L: tuple, target: int):
    res = []
    for sub_L in L:
        vector = [value_list[x] for x in sub_L]
        sum_vector = sum(vector)
        if sum_vector == target:
            res.append([tuple(vector), sum_vector])
    return res

# test_find_1 = find(test2, 42)

solution = find(tuple(index_container), target=780)

'''
From var solution we conclude that min(max(class)) = 24 (276 edges)
'''