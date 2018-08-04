import itertools
import numpy as np

def Mideng(li):
    if type(li)!=list:
        return
    if len(li)==1:
        return [li]
    result=[]
    for i in range(0,len(li[:])):
        bak=li[:]
        head=bak.pop(i) #head of the recursive-produced value
        for j in Mideng(bak):
            j.insert(0,head)
            result.append(j)
    return result

def MM(n):
    if type(n)!=list:
        return
    return Mideng(list(n))

def full_arrange(list_n, m):
    if m > 3 or m == 1:
        print('can not do')
        return
    total_list = []
    tmp = list(itertools.combinations(list(list_n), m))
    for i in tmp:
        i = list(i)
        small_list = MM(i)
        for j in small_list:
            total_list.append(j)
    shink = 1
    if shink == 1:
        tmp = list(itertools.combinations(list(list_n), m - 1))
        tmp_change = []
        for i in tmp:
            i = list(i)
            small_tmp = list(itertools.combinations(i, shink))
            for j in small_tmp:
                j = list(j)
                tmp_change.append(i + j)
        for i in tmp_change:
            small_list = MM(i)
            for j in small_list:
                total_list.append(j)
        shink = shink + 1

    if shink == 2 and shink < m:
        for i in list(list_n):
            tmp = (np.ones((1,m)) * i).tolist()[0]
            tmp_new = [int(i) for i in tmp]
            total_list.append(tmp_new)

    tuple_list = [tuple(i) for i in total_list]
    total_list = list(set(tuple_list))
    total_list = sorted(total_list, key=lambda x: x[0])
    total_list = [list(i) for i in total_list]
    return(total_list)


if __name__ == '__main__':
    a = full_arrange([0,3], 3)
    count = 0
    for i in a:
        print(i)
        count = count + 1
    print('组合一共 ' + str(count) + ' 个')